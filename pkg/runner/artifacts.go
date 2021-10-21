package runner

import (
	v1batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"strconv"
)

func (r *runner) ListArtifacts(pipelineId string) ([]string, error) {
	return r.rc.ListArtifacts(pipelineId)
}

func (r *runner) GetArtifact(pipelineId, name string) ([]byte, error) {
	return r.rc.GetArtifact(pipelineId, name)
}

func (r *runner) withArtifacts(job *v1batch.Job, pipelineId, jobName string) *v1batch.Job {
	// add additional volumes
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes,
		// volume for saving artifacts
		v1.Volume{
			Name: "artifacts",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{
					Medium: "",
				},
			},
		})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts,
		v1.VolumeMount{
			Name:      "artifacts",
			ReadOnly:  false,
			MountPath: "/artifacts",
		})
	au := v1.Container{
		Name:  "artifact-uploader",
		Image: r.cfg.Artifacts.ArtifactUploaderImage,
		VolumeMounts: []v1.VolumeMount{{
			Name:      "artifacts",
			ReadOnly:  false,
			MountPath: "/artifacts",
		}},
		Env: addOrReplaceEnvVars([]v1.EnvVar{{
			Name: "POD_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		}, {
			Name: "POD_NAMESPACE",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		}}, map[string]string{
			"PIPELINE_ID":   pipelineId,
			"JOB_NAME":      jobName,
			"REDIS_ADDRESS": r.cfg.Redis.Address,
			"REDIS_CLUSTER": strconv.FormatBool(r.cfg.Redis.IsCluster),
		}),
		ImagePullPolicy: v1.PullAlways,
	}
	job.Spec.Template.Spec.Containers = append([]v1.Container{au}, job.Spec.Template.Spec.Containers...)
	if r.cfg.Artifacts.ImagePullSecret != "" {
		job.Spec.Template.Spec.ImagePullSecrets = append(job.Spec.Template.Spec.ImagePullSecrets,
			v1.LocalObjectReference{Name: r.cfg.Artifacts.ImagePullSecret})
	}
	return job
}
