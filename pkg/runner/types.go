package runner

import (
	"encoding/json"
	"github.com/hitman99/peppercd/internal/redis"
	"time"
)

type PipelineDef struct {
	Name string   `yaml:"name" json:"name"`
	Jobs []JobDef `yaml:"jobs" json:"jobs"`
}

func (pd PipelineDef) DeepCopy() PipelineDef {
	b, _ := json.Marshal(pd)
	copy := PipelineDef{}
	_ = json.Unmarshal(b, &copy)
	return copy
}

type JobDef struct {
	Name         string            `yaml:"name" json:"name"`
	Image        string            `yaml:"image" json:"image"`
	Script       string            `yaml:"script" json:"script"`
	Env          map[string]string `yaml:"env" json:"env"`
	Secrets      []Secret          `yaml:"secrets" json:"secrets"`
	PullSecrets  []string          `yaml:"pullSecrets" json:"pullSecrets"`
	AllowFailure bool              `yaml:"allowFailure" json:"allowFailure"`
}

// If MountPath is specified, then the secret will be mounted as file
// If MountPath is not specified, secret will be injected into environment
type Secret struct {
	Name string `yaml:"name" json:"name"`
	// Kubernetes secret name to refer to
	ValueRef string `yaml:"valueRef" json:"valueRef"`
	// Key inside the secret. If not specified, all data will be taken and exposed either via ENV or mounted to specified path
	Key string `yaml:"key" json:"key"`
	// If this is specified, secret will be mounted to specified path
	// If key is specified, the resulting path will be Mountpath/Key
	MountPath string `yaml:"mountPath" json:"mountPath"`
}

type Pipeline struct {
	PipelineDef   `json:"def"`
	Jobs          []*Job         `json:"jobs"`
	UID           string         `json:"uid"`
	Status        PipelineStatus `json:"status"`
	StatusContext string         `json:"statusContext"`
}

type Job struct {
	JobDef         `json:"def"`
	UID            string    `json:"uid"`
	Status         JobStatus `json:"status"`
	Done           bool      `json:"done"`
	Name           string    `json:"name"`
	StartTime      time.Time `json:"startTime"`
	CompletionTime time.Time `json:"completionTime"`
}

type PipelineStatus int

const (
	PIPELINE_RUNNING PipelineStatus = iota
	PIPELINE_FAILED
	PIPELINE_COMPLETED
)

func (ps PipelineStatus) String() string {
	return [...]string{"running", "failed", "completed"}[ps]
}

func (ps PipelineStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(ps.String())
}

func (ps *PipelineStatus) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*ps = ps.FromString(str)
	return nil
}

func (ps PipelineStatus) FromString(status string) PipelineStatus {
	switch status {
	case "running":
		return PIPELINE_RUNNING
	case "failed":
		return PIPELINE_FAILED
	case "completed":
		return PIPELINE_COMPLETED
	default:
		return PIPELINE_RUNNING
	}
}

type JobStatus int

const (
	JOB_ACTIVE JobStatus = iota
	JOB_FAILED
	JOB_SUCCEEDED
	JOB_UNKNOWN
	JOB_SCHEDULED
)

func (js JobStatus) String() string {
	return [...]string{"Active", "Failed", "Succeeded", "Unknown", "Scheduled"}[js]
}

func (js JobStatus) FromString(status string) JobStatus {
	switch status {
	case "Active":
		return JOB_ACTIVE
	case "Failed":
		return JOB_FAILED
	case "Succeeded":
		return JOB_SUCCEEDED
	case "Unknown":
		return JOB_UNKNOWN
	case "Scheduled":
		return JOB_SCHEDULED
	default:
		return JOB_ACTIVE
	}
}

func (js JobStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(js.String())
}

func (js *JobStatus) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	*js = js.FromString(str)
	return nil
}

type KubeConfig struct {
	// setting this to true means that the application is running outside of the cluster
	DevMode bool `yaml:"devMode"`
	// this must be set if dev mode is set to true
	Kubeconfig string `yaml:"kubeconfig"`
	// namespace to run jobs in
	JobNamespace string `yaml:"jobNamespace"`
}

type RedisConfig struct {
	IsCluster bool   `yaml:"isCluster"`
	Address   string `yaml:"address"`
}

type ArtifactConfig struct {
	ArtifactUploaderImage string `yaml:"artifactUploaderImage"`
	ImagePullSecret       string `yaml:"imagePullSecret"`
}

type Config struct {
	Kubernetes KubeConfig  `yaml:"kubernetes"`
	Redis      RedisConfig `yaml:"redis"`
	// image to use for artifact scraping
	Artifacts ArtifactConfig `yaml:"artifacts"`
}

type BuildState struct {
	Pipeline     *Pipeline
	BuildContext *BuildContext
}

type livePipeline struct {
	lease        *redis.PipelineLease
	buildContext *BuildContext
	updates      chan *BuildState
	p            *Pipeline
}
