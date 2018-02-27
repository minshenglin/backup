package repo

import (
	"syscall"
)

type Repository struct {
	Path   string  `json:"path"`
	Free   uint64  `json:"free_space"`
	Total  uint64  `json:"total_space"`
}

func NewRepository(path string) (*Repository, error) {
	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(path, &fs); err != nil {
		return &Repository{}, err
	}
	free := fs.Bfree * uint64(fs.Bsize)
	total := fs.Blocks * uint64(fs.Bsize)
	return &Repository{path, free, total}, nil
}
