package runner

import (
	v1batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"path"
)

func injectSecrets(secrets []Secret, job *v1batch.Job, containerName string) *v1batch.Job {
	var container *v1.Container
	for idx, c := range job.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			container = &job.Spec.Template.Spec.Containers[idx]
			break
		}
	}
	// bail
	if container == nil {
		return job
	}

	for _, sec := range secrets {
		if len(sec.ValueRef) == 0 {
			continue
		}
		if len(sec.MountPath) != 0 {
			var items []v1.KeyToPath
			if len(sec.Key) == 0 {
				// mount all keys to directory
				container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
					Name:      sec.Name,
					ReadOnly:  true,
					MountPath: sec.MountPath,
				})
			} else {
				// mount one key only
				container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
					Name:      sec.Name,
					ReadOnly:  true,
					MountPath: path.Join(sec.MountPath, sec.Key),
					SubPath:   sec.Key,
				})
				items = []v1.KeyToPath{{
					Key:  sec.Key,
					Path: sec.Key,
				}}
			}
			job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, v1.Volume{
				Name: sec.Name,
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: sec.ValueRef,
						Items:      items,
					},
				},
			})
		} else {
			// inject all secrets to environment
			if len(sec.Key) == 0 {
				container.EnvFrom = append(container.EnvFrom, v1.EnvFromSource{
					SecretRef: &v1.SecretEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: sec.ValueRef,
						},
					},
				})
			} else {
				container.Env = append(container.Env, v1.EnvVar{
					Name: sec.Name,
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: sec.ValueRef,
							},
							Key: sec.Key,
						},
					},
				})
			}
		}
	}
	return job
}
