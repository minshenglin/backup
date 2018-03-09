package job

import (
	"backup/redis"
	"backup/utils"
	"time"
	"encoding/json"
)

type Job struct {
	Uuid         string  `json:"uuid"`
	CreatedTime  uint64  `json:"created_time"`
	Tasks        Task    `json:"task"`
}

type Task struct {
	Type         string  `json:"type"`
	Pool         string  `json:"pool"`
	Image        string  `json:"image"`
	RepoUuid     string  `json:"repo_uuid"`
}

func NewJob(data string) (*Job, error) {
	job := Job{}
	err := json.Unmarshal([]byte(data), &job)
	if err != nil {
		return &Job{}, err
	}
	return &job, nil
}

type JobHandler struct {
	rh *redis.RedisHandler
}

func NewJobHandler(redisAddress string) *JobHandler{
	rh := redis.New(redisAddress, "job")
	return &JobHandler{rh}
}

func (jh *JobHandler) CreateJob(task Task) (string, error) {
	uuid, err := utils.MakeUuid()
	if err != nil {
		return "", err
	}
	timestamp := uint64(time.Now().Unix())

	job := Job{uuid, timestamp, task}
	err = jh.rh.Add(job, uuid)
	return uuid, err
}

func (jh *JobHandler) ListJob() ([]Job, error) {
	list, err := jh.rh.List()
	if err != nil {
		return []Job{}, err
	}

	jobs := make([]Job, 0)
	for _, s := range list {
		job := Job{}
		err := json.Unmarshal([]byte(s), &job)
		if err != nil {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (jh *JobHandler) UpdateJobProgress(uuid string, percentage int) error {
	err := jh.rh.UpdateProgress(uuid, percentage)
	return err
}
