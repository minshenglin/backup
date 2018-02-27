package job

import (
	"crypto/rand"
	"github.com/garyburd/redigo/redis"
	//"backup/ceph"
	"time"
	"fmt"
	"encoding/json"
)

type Job struct {
	Uuid         string  `json:"uuid"`
	CreatedTime  uint64  `json:"created_time"`
	Type         string  `json:"type"`
	Pool         string  `json:"pool"`
	Image        string  `json:"image"`
	Path         string  `json:"path"`
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
	return &JobHandler{redisAddress, "job-"}
}

func (jh *JobHandler) CreateJob(kind string, pool string, image string, path string) (string, error) {
	uuid, err := jh.makeUuid()
	if err != nil {
		return "", err
	}
	timestamp := uint64(time.Now().Unix())

	job := Job{uuid, timestamp, kind, pool, image, path}

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
