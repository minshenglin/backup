package job

import (
	"crypto/rand"
	"github.com/garyburd/redigo/redis"
	//"backup/ceph"
	"time"
	"fmt"
	"encoding/json"
	"log"
	"errors"
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
	Path         string  `json:"path"`
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
	redisAddress string
	prefix string
}

func (jh *JobHandler) makeUuid() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	uuid := fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid, nil
}

func NewJobHandler(redisAddress string) *JobHandler{
	return &JobHandler{redisAddress, "job:"}
}

func (jh *JobHandler) CreateJob(task Task) (string, error) {
	uuid, err := jh.makeUuid()
	if err != nil {
		return "", err
	}
	timestamp := uint64(time.Now().Unix())

	job := Job{uuid, timestamp, task}

	b, err := json.Marshal(job)
	if err != nil {
		return "", err
	}
	client, err := redis.Dial("tcp", jh.redisAddress)
	if err != nil {
		return "", err
	}
	defer client.Close()

	key := jh.prefix + uuid
	_, err = client.Do("SET", key, string(b))
	if err != nil {
		return "", err
	}

	client.Do("RPUSH", jh.prefix + "list", uuid)
	return uuid, nil
}

func (jh *JobHandler) LoadJob(uuid string) (string, error) {

	client, err := redis.Dial("tcp", jh.redisAddress)
	if err != nil {
		return "", err
	}
	defer client.Close()

	key := jh.prefix + uuid
	v, err := redis.String(client.Do("GET", key))
	if err != nil {
		return "", err
	}
	return v, nil
}

func (jh *JobHandler) ListJob(length int) ([]Job, error) {
	if length < 0 {
		return []Job{}, errors.New("invalid length")
	}
	client, err := redis.Dial("tcp", jh.redisAddress)
	if err != nil {
		return []Job{}, err
	}
	defer client.Close()

	v, err := redis.Values(client.Do("LRANGE", jh.prefix + "list", -length, -1)) // take least n element
	if err != nil {
		return []Job{}, err
	}
	log.Println("Job list loaded done, length is", len(v))
	jobs := make([]Job, 0)
	for _, uuid := range v {
		key := jh.prefix + string(uuid.([]byte))
		v, err := redis.String(client.Do("GET", key))
		if err != nil {
			continue
		}
		j, err := NewJob(v)
		if err != nil {
			continue
		}
		jobs = append(jobs, *j)
	}
	return jobs, nil
}

func (jh *JobHandler) UpdateJobProgress(uuid string, percentage int) error {
	client, err := redis.Dial("tcp", jh.redisAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Do("SET", jh.prefix + uuid + "-progress", percentage)
	return err
}
