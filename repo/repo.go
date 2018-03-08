package repo

import (
	"encoding/json"
	"errors"
	"backup/redis"
	"backup/utils"
	"os"
	"syscall"
)

type Repository struct {
	Uuid  string `json:"uuid"`
	Name  string `json:"name"`
	Path  string `json:"path"`
	Free  uint64 `json:"free_space,omitempty"`
	Total uint64 `json:"total_space,omitempty"`
}

type RepositoryHandler struct {
	redis *redis.RedisHandler
	namespace    string
}

func NewRepositoryHandler(redisAddress string) *RepositoryHandler {
	rh := redis.New(redisAddress)
	return &RepositoryHandler{rh, "repo"}
}

func (rh *RepositoryHandler) AddRepo(repo Repository) (string, error) {
	f, err := os.Stat(repo.Path)
	if err != nil {
		return "", err
	}
	if !f.IsDir() {
		return "", errors.New("path " + repo.Path + " is not directory")
	}

	uuid, err := utils.MakeUuid() 
	if err != nil {
		return "", err
	}
	repo.Uuid = uuid
	err = rh.redis.Add(repo, rh.namespace, uuid)
	return uuid, err
}

func (rh *RepositoryHandler) ListRepo() ([]Repository, error) {
	list, err := rh.redis.List(rh.namespace)
	if err != nil {
		return []Repository{}, err
	}

	repos := make([]Repository, 0)
	for _, s := range list {
		repo := Repository{}
		err := json.Unmarshal([]byte(s), &repo)
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
