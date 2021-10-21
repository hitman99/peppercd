package runner

import (
	"context"
	log "github.com/sirupsen/logrus"
	v1batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	v1meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func mustNewKubeClient(cfg *KubeConfig) *kubernetes.Clientset {
	var (
		err        error
		restConfig *rest.Config
	)
	if cfg.DevMode {
		restConfig, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		log.WithError(err).Fatal("cannot create kubeclient")
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.WithError(err).Fatal("cannot create kubeclient")
	}

	return clientset
}

func (r *runner) runAndWatchJob(j *JobDef, pipelineId string) (watch.Interface, error) {
	var (
		kubeJob *v1batch.Job
		bareJob bool = len(j.Script) == 0
		cmName  string
		retries int32 = 0
		ctx           = context.TODO()
	)

	if bareJob {
		kubeJob = r.jobWithoutScript(j, pipelineId)
	} else {
		var err error
		cmName, err = r.prepareScriptForJob(j.Script)
		if err != nil {
			return nil, err
		}
		kubeJob = r.jobWithScript(j, pipelineId, cmName)
	}
	kubeJob.Spec.BackoffLimit = &retries
	job, err := r.kube.BatchV1().Jobs(r.cfg.Kubernetes.JobNamespace).Create(ctx, kubeJob, v1meta.CreateOptions{})
	if err != nil {
		return nil, err
	}
	// add owner refs to configmaps for tracking and easier cleanup
	if !bareJob {
		if err := r.addConfigMapRef(cmName, job); err != nil {
			rmErr := r.removeConfigMap(cmName)
			if rmErr != nil {
				return nil, rmErr
			}
			return nil, err
		}
	}
	wi, err := r.getJobWatcher(job.Name)
	if err != nil {
		return nil, err
	}
	return wi, nil
}

func (r *runner) getJobWatcher(jobName string) (watch.Interface, error) {
	ctx := context.TODO()
	return r.kube.BatchV1().Jobs(r.cfg.Kubernetes.JobNamespace).Watch(ctx, v1meta.ListOptions{
		FieldSelector: "metadata.name=" + jobName,
		Watch:         true,
		Limit:         1,
	})
}

func (r *runner) getJob(jobName string) (*v1batch.Job, error) {
	ctx := context.TODO()
	job, err := r.kube.BatchV1().Jobs(r.cfg.Kubernetes.JobNamespace).Get(ctx, jobName, v1meta.GetOptions{})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (r *runner) prepareScriptForJob(data string) (string, error) {
	ctx := context.TODO()
	cm, err := r.kube.CoreV1().ConfigMaps(r.cfg.Kubernetes.JobNamespace).Create(ctx, &v1.ConfigMap{
		ObjectMeta: v1meta.ObjectMeta{
			GenerateName: "pipe",
		},
		Data:       map[string]string{"job.sh": data},
		BinaryData: nil,
	}, v1meta.CreateOptions{})
	if err != nil {
		return "", err
	}
	return cm.Name, nil
}

func (r *runner) addConfigMapRef(cmName string, job *v1batch.Job) error {
	ctx := context.TODO()
	cm, err := r.kube.CoreV1().ConfigMaps(r.cfg.Kubernetes.JobNamespace).Get(ctx, cmName, v1meta.GetOptions{})
	if err != nil {
		return err
	}
	controlled := true
	cm.ObjectMeta.SetOwnerReferences([]v1meta.OwnerReference{{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Name:       job.Name,
		UID:        job.UID,
		Controller: &controlled,
	}})
	_, err = r.kube.CoreV1().ConfigMaps(r.cfg.Kubernetes.JobNamespace).Update(ctx, cm, v1meta.UpdateOptions{})
	return err
}

func (r *runner) removeConfigMap(name string) error {
	ctx := context.TODO()
	return r.kube.CoreV1().ConfigMaps(r.cfg.Kubernetes.JobNamespace).Delete(ctx, name, v1meta.DeleteOptions{})
}

func (r *runner) jobWithScript(def *JobDef, pipelineId, cmName string) *v1batch.Job {
	var defaultMode int32 = 448
	pullSecrets := []v1.LocalObjectReference{}
	for _, secret := range def.PullSecrets {
		pullSecrets = append(pullSecrets, v1.LocalObjectReference{Name: secret})
	}
	job := r.withArtifacts(&v1batch.Job{
		ObjectMeta: v1meta.ObjectMeta{
			GenerateName: def.Name + "-",
		},
		Spec: v1batch.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:    def.Name,
						Image:   def.Image,
						Command: []string{"/bin/job.sh"},
						VolumeMounts: []v1.VolumeMount{{
							Name:      "job",
							ReadOnly:  true,
							MountPath: "/bin/job.sh",
							SubPath:   "job.sh",
						}},
						Env: addEnvVars(def.Env),
					}},
					RestartPolicy: "Never",
					Volumes: []v1.Volume{{
						Name: "job",
						VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{
									Name: cmName,
								},
								Items: []v1.KeyToPath{{
									Key:  "job.sh",
									Path: "job.sh",
									Mode: &defaultMode,
								}},
							},
						},
					}},
					ServiceAccountName: "artifact-uploader",
					ImagePullSecrets:   pullSecrets,
				},
			},
		},
	}, pipelineId, def.Name)
	return injectSecrets(def.Secrets, job, def.Name)
}

func (r *runner) jobWithoutScript(def *JobDef, pipelineId string) *v1batch.Job {
	pullSecrets := []v1.LocalObjectReference{}
	for _, secret := range def.PullSecrets {
		pullSecrets = append(pullSecrets, v1.LocalObjectReference{Name: secret})
	}
	job := r.withArtifacts(&v1batch.Job{
		ObjectMeta: v1meta.ObjectMeta{
			GenerateName: def.Name + "-",
		},
		Spec: v1batch.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:  def.Name,
						Image: def.Image,
						Env:   addEnvVars(def.Env),
					}},
					RestartPolicy:      "Never",
					ServiceAccountName: "artifact-uploader",
					ImagePullSecrets:   pullSecrets,
				},
			},
		},
	}, pipelineId, def.Name)
	return injectSecrets(def.Secrets, job, def.Name)
}

func addOrReplaceEnvVars(env []v1.EnvVar, additionalVars map[string]string) (allVars []v1.EnvVar) {
	// scan existing vars and replace Value if there is a match on Name
	// append remaining vars to the end of slice
	allVars = make([]v1.EnvVar, len(env))
	copy(allVars, env)
	for name, value := range additionalVars {
		replaced := false
		for _, v := range allVars {
			if v.Name == name {
				v.Value = value
				replaced = true
				break
			}
		}
		if !replaced {
			allVars = append(allVars, v1.EnvVar{
				Name:  name,
				Value: value,
			})
		}
	}
	return
}

func addEnvVars(inputVars map[string]string) (outputVars []v1.EnvVar) {
	for name, value := range inputVars {
		outputVars = append(outputVars, v1.EnvVar{
			Name:  name,
			Value: value,
		})
	}
	return
}
