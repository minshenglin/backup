package repo

import (
	"encoding/json"
	"errors"
	"github.com/garyburd/redigo/redis"
	"os"
	"log"
	"syscall"
)

type Repository struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Free  uint64 `json:"free_space,omitempty"`
	Total uint64 `json:"total_space,omitempty"`
}

type RepositoryHandler struct {
	redisAddress string
	namespace    string
}

func NewRepositoryHandler(redisAddress string) *RepositoryHandler {
	return &RepositoryHandler{redisAddress, "repo:"}
}

func (rh *RepositoryHandler) AddRepo(repo Repository) error {

	log.Println("Connect to redis")
	client, err := redis.Dial("tcp", rh.redisAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	log.Println("Check repo path", repo.Path, "is direcory")

	f, err := os.Stat(repo.Path)
	if err != nil {
		return err
	}
	if !f.IsDir() {
		return errors.New("path " + repo.Path + " is not directory")
	}

	b, err := json.Marshal(repo)
	if err != nil {
		return nil
	}

	log.Println("Push Repo info string to redis")
	_, err = client.Do("RPUSH", rh.namespace+"list", string(b))
	return err
}

func (rh *RepositoryHandler) ListRepo() ([]Repository, error) {
	client, err := redis.Dial("tcp", rh.redisAddress)
	if err != nil {
		return []Repository{}, err
	}
	defer client.Close()

	v, err := redis.Values(client.Do("LRANGE", rh.namespace + "list" ,0, -1)) // take all element
	if err != nil {
		return []Repository{}, err
	}
	log.Println("Repo list loaded done, length is", len(v))

	repos := make([]Repository, 0)
	for _, b := range v {
		repo := Repository{}
		err := json.Unmarshal(b.([]byte), &repo)
		if err != nil {
			continue
		}

		repo.Free, repo.Total, err = rh.getSpaceInfo(repo.Path)
		if err != nil {
			continue
		}
		repos = append(repos, repo)
	}
	return repos, nil
}

func (rh *RepositoryHandler) getSpaceInfo(path string) (uint64, uint64, error) {

	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, 0, err
	}
	free := fs.Bfree * uint64(fs.Bsize)
	total := fs.Blocks * uint64(fs.Bsize)
	return free, total, nil
}
