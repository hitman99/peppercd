package runner

import (
	"errors"
	"github.com/gofrs/uuid"
	"github.com/hitman99/peppercd/internal/redis"
	log "github.com/sirupsen/logrus"
	v1batch "k8s.io/api/batch/v1"
	v1meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"time"
)

type Interface interface {
	RunPipeline(req *Request) (<-chan *BuildState, error)
	GetPipeline(uid string) (*Pipeline, error)
	ListArtifacts(pipelineId string) ([]string, error)
	GetArtifact(pipelineId, name string) ([]byte, error)
	RetryFailedJobs(pipelineId string, ctx *BuildContext) (<-chan *BuildState, error)
}

type runner struct {
	cfg    *Config
	kube   *kubernetes.Clientset
	id     string
	rc     redis.Client
	logger *log.Logger
}

func (r *runner) GetPipeline(pipelineId string) (*Pipeline, error) {
	p := &Pipeline{}
	err := r.rc.GetPipeline(pipelineId, p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (r *runner) RunPipeline(req *Request) (<-chan *BuildState, error) {
	if req == nil {
		return nil, errors.New("invalid build request")
	}

	jobs := make([]*Job, 0, len(req.PipelineDef.Jobs))
	for _, j := range req.PipelineDef.Jobs {
		jobs = append(jobs, &Job{
			JobDef: j,
			UID:    "",
			Status: JOB_UNKNOWN,
			Done:   false,
		})
	}
	p := &Pipeline{
		PipelineDef: *req.PipelineDef,
		UID:         uuid.Must(uuid.NewV4()).String(),
		Status:      0,
		Jobs:        jobs,
	}
	r.logger.WithField("pipelineUid", p.UID).Debug("creating pipeline")
	lease, err := r.rc.CreatePipeline(p.UID, r.id, p)
	if err != nil {
		return nil, err
	}
	lp := &livePipeline{
		lease:        lease,
		buildContext: req.Context,
		updates:      make(chan *BuildState),
		p:            p,
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-time.After(redis.LEASE_DURATION / 2):
				if lp.lease.Expires.Before(time.Now().Add(redis.LEASE_DURATION)) {
					extendedLease, err := r.rc.ExtendLease(r.id, lp.p.UID)
					if err != nil {
						r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to extend lease for pipeline")
						// cannot extend lease, this will break saving pipeline state to REDIS, exiting
						return
					} else {
						lp.lease = extendedLease
						r.logger.WithFields(log.Fields{"pipelineUid": lp.p.UID, "expiration": lp.lease.Expires.String()}).Debug("lease for pipeline extended")
					}
				}
			case <-stop:
				return
			}
		}
	}()
	go func() {
		defer close(lp.updates)
		defer close(stop)
		defer func() {
			if rec := recover(); rec != nil {
				r.logger.WithError(rec.(error)).WithField("pipelineUid", lp.p.UID).Error("Pipeline failed")
				r.updateLivePipeline(lp)
				return
			}
		}()
		defer func() {
			if lp.lease.Expires.After(time.Now()) {
				if err := r.rc.TerminateLease(lp.p.UID); err != nil {
					r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to terminate lease for pipeline")
				} else {
					r.logger.WithField("pipelineUid", lp.p.UID).Debug("terminating lease for pipeline")
				}
			}
		}()
		for jobId, job := range lp.p.Jobs {
			watcher, err := r.runAndWatchJob(&job.JobDef, lp.p.UID)
			if err != nil {
				lp.p.Status = PIPELINE_FAILED
				lp.p.StatusContext = err.Error()
				r.updateLivePipeline(lp)
				return
			}

			for {
				shouldReWatch := r.watchJob(watcher, jobId, job, lp)
				if !shouldReWatch {
					if lp.p.Status != PIPELINE_FAILED {
						break
					} else {
						r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("pipeline execution stopped due to failed job")
						return
					}
				}
				r.logger.WithField("jobName", job.Name).Info("job watcher failed, will re-initialize job watcher")
				// check the status first in case it has changed and the update was missed for some reason
				kubeJob, err := r.getJob(job.Name)
				if err != nil {
					lp.p.Status = PIPELINE_FAILED
					lp.p.StatusContext = err.Error()
					r.updateLivePipeline(lp)
					r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("cannot get job status, pipeline execution stopped")
					return
				}
				lp.p.Jobs[jobId] = r.updateJob(kubeJob, job)
				r.updateLivePipeline(lp)
				if kubeJob.Status.Succeeded == 1 {
					r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job succeeded")
					break
				}
				if kubeJob.Status.Failed == 1 {
					r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job failed")
					return
				}
				watcher, err = r.getJobWatcher(job.Name)
				if err != nil {
					lp.p.Status = PIPELINE_FAILED
					lp.p.StatusContext = err.Error()
					r.updateLivePipeline(lp)
					r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job failed, pipeline execution stopped")
					return
				}
			}
		}
		if lp.p.Status != PIPELINE_FAILED {
			// all jobs finished
			lp.p.Status = PIPELINE_COMPLETED
			r.updateLivePipeline(lp)
			r.logger.WithField("pipelineUid", lp.p.UID).Debug("pipeline finished successfully")
		}
	}()
	return lp.updates, nil
}

func (r *runner) RetryFailedJobs(pipelineId string, ctx *BuildContext) (<-chan *BuildState, error) {
	r.logger.WithField("pipelineUid", pipelineId).Debug("looking up pipeline")
	p := &Pipeline{}
	err := r.rc.GetPipeline(pipelineId, p)
	if err != nil {
		return nil, err
	}

	lease, err := r.rc.AcquireLease(r.id, pipelineId)
	if err != nil {
		return nil, err
	}
	p.Status = PIPELINE_RUNNING
	lp := &livePipeline{
		lease:        lease,
		buildContext: ctx,
		updates:      make(chan *BuildState),
		p:            p,
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-time.After(redis.LEASE_DURATION / 2):
				if lp.lease.Expires.Before(time.Now().Add(redis.LEASE_DURATION)) {
					extendedLease, err := r.rc.ExtendLease(r.id, lp.p.UID)
					if err != nil {
						r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to extend lease for pipeline")
						// cannot extend lease, this will break saving pipeline state to REDIS, exiting
						return
					} else {
						lp.lease = extendedLease
						r.logger.WithFields(log.Fields{"pipelineUid": lp.p.UID, "expiration": lp.lease.Expires.String()}).Debug("lease for pipeline extended")
					}
				}
			case <-stop:
				return
			}
		}
	}()

	go func() {
		defer close(lp.updates)
		defer close(stop)
		defer func() {
			if lp.lease.Expires.After(time.Now()) {
				if err := r.rc.TerminateLease(lp.p.UID); err != nil {
					r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to terminate lease for pipeline")
				} else {
					r.logger.WithField("pipelineUid", lp.p.UID).Debug("terminating lease for pipeline")
				}
			}
		}()
		defer func() {
			if rec := recover(); rec != nil {
				r.logger.WithError(rec.(error)).WithField("pipelineUid", lp.p.UID).Error("Pipeline failed")
				r.updateLivePipeline(lp)
				return
			}
		}()
		for jobId, job := range lp.p.Jobs {
			if job.Status == JOB_SUCCEEDED {
				r.logger.WithField("jobName", job.Name).Info("skipping job because it already succeeded")
				r.updateLivePipeline(lp)
				continue
			}
			watcher, err := r.runAndWatchJob(&job.JobDef, lp.p.UID)
			if err != nil {
				lp.p.Status = PIPELINE_FAILED
				lp.p.StatusContext = err.Error()
				r.updateLivePipeline(lp)
				return
			}

			for {
				shouldReWatch := r.watchJob(watcher, jobId, job, lp)
				if !shouldReWatch {
					if lp.p.Status != PIPELINE_FAILED {
						break
					} else {
						r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("pipeline execution stopped due to failed job")
						return
					}
				}
				r.logger.WithField("jobName", job.Name).Info("job watcher failed, will re-initialize job watcher")
				watcher, err = r.getJobWatcher(job.Name)
				if err != nil {
					lp.p.Status = PIPELINE_FAILED
					lp.p.StatusContext = err.Error()
					r.updateLivePipeline(lp)
					r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job failed, pipeline execution stopped")
					return
				}
			}
		}
		if lp.p.Status != PIPELINE_FAILED {
			// all jobs finished
			lp.p.Status = PIPELINE_COMPLETED
			r.updateLivePipeline(lp)
			r.logger.WithField("pipelineUid", lp.p.UID).Debug("pipeline finished successfully")
		}
	}()
	return lp.updates, nil
}

func (r *runner) updateLivePipeline(lp *livePipeline) {
	newLease, err := r.rc.SavePipeline(lp.p.UID, lp.p)
	if err != nil {
		if err == redis.LeaseExpired {
			_, err := r.rc.ExtendLease(r.id, lp.p.UID)
			if err != nil {
				r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to re-acquire expired lease for pipeline")
			}
		}
		if err == redis.LeaseNotFound {
			_, err := r.rc.AcquireLease(r.id, lp.p.UID)
			if err != nil {
				r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to re-acquire expired lease for pipeline")
			}
		}
		newLease, err := r.rc.SavePipeline(lp.p.UID, lp.p)
		if err != nil {
			r.logger.WithError(err).WithField("pipelineUid", lp.p.UID).Error("failed to save pipeline after re-acquiring lease")
		}
		lp.lease = newLease
	}
	lp.lease = newLease
	lp.updates <- &BuildState{
		Pipeline:     lp.p,
		BuildContext: lp.buildContext,
	}
}

func (r *runner) updateJob(kubeJob *v1batch.Job, job *Job) *Job {
	currentStatus := JOB_UNKNOWN
	if kubeJob.Status.Active == 1 {
		currentStatus = JOB_ACTIVE
	}
	if kubeJob.Status.Failed == 1 {
		currentStatus = JOB_FAILED
	}
	if kubeJob.Status.Succeeded == 1 {
		currentStatus = JOB_SUCCEEDED
		job.Done = true
	}
	job.Status = currentStatus
	if kubeJob.Status.StartTime != nil {
		job.StartTime = kubeJob.Status.StartTime.UTC()
	} else {
		// not sure about this, but it will do for now
		if job.StartTime.IsZero() {
			job.Status = JOB_SCHEDULED
		}
	}
	if kubeJob.Status.CompletionTime != nil {
		job.CompletionTime = kubeJob.Status.CompletionTime.UTC()
	}
	job.Name = kubeJob.Name
	return job
}

func New(cfg *Config, runnerId string, logger *log.Logger) Interface {
	return &runner{
		cfg:    cfg,
		kube:   mustNewKubeClient(&cfg.Kubernetes),
		id:     runnerId,
		rc:     redis.MustNewClient(cfg.Redis.IsCluster, cfg.Redis.Address),
		logger: logger,
	}
}

func (r *runner) watchJob(watcher watch.Interface, jobId int, job *Job, lp *livePipeline) bool {
	for e := range watcher.ResultChan() {
		switch e.Type {
		case watch.Added:
			j := e.Object.DeepCopyObject().(*v1batch.Job)
			lp.p.Jobs[jobId] = r.updateJob(j, job)
			r.updateLivePipeline(lp)
		case watch.Modified:
			j := e.Object.DeepCopyObject().(*v1batch.Job)
			job.UID = string(j.UID)
			lp.p.Jobs[jobId] = r.updateJob(j, job)
			r.updateLivePipeline(lp)
			// if job is not allowed to fail, stop pipeline
			if j.Status.Failed == 1 && !job.AllowFailure {
				lp.p.Status = PIPELINE_FAILED
				lp.p.StatusContext = j.Status.String()
				r.updateLivePipeline(lp)
				watcher.Stop()
				r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job failed")
				return false
			}
			if j.Status.Succeeded == 1 {
				r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job succeeded")
				r.updateLivePipeline(lp)
				watcher.Stop()
			}
		case watch.Error:
			s := e.Object.DeepCopyObject().(*v1meta.Status)
			// in case of internal error, try to restart watching
			if s.Code == 500 {
				// terminate old watcher
				watcher.Stop()
				return true
			}
			job.Status = JOB_FAILED
			lp.p.Jobs[jobId] = job
			if !job.AllowFailure {
				lp.p.Status = PIPELINE_FAILED
				lp.p.StatusContext = s.Status
				r.updateLivePipeline(lp)
				watcher.Stop()
				r.logger.WithField("pipelineUid", lp.p.UID).WithField("jobName", job.Name).Debug("job failed")
				return false
			}
			r.updateLivePipeline(lp)
			watcher.Stop()
		case watch.Deleted:
			r.logger.WithField("jobName", job.Name).Info("job was deleted externally")
			job.Status = JOB_FAILED
			lp.p.Jobs[jobId] = job
			lp.p.Status = PIPELINE_FAILED
			lp.p.StatusContext = "job was deleted"
			r.updateLivePipeline(lp)
			watcher.Stop()
			return false
		default:
			r.logger.WithField("jobName", job.Name).Info("unexpected job state")
		}
	}
	return false
}
