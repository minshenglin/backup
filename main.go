package main

import (
	"backup/ceph"
	"backup/repo"
	"backup/job"
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
)

func GetPools(w http.ResponseWriter, r *http.Request) {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	// connect to ceph cluster
	handler, err := ceph.NewCephHandler()
	if err != nil {
		logger.Println("Rados connect failed:", err)
	}
	logger.Println("Rados connect successily")

	// list pools
	pools, err := handler.ListPool()
	if err != nil {
		logger.Println("List pool failed:", err)
	}
	json.NewEncoder(w).Encode(pools)
}

func GetImages(w http.ResponseWriter, r *http.Request) {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	// connect to ceph cluster
	handler, err := ceph.NewCephHandler()
	if err != nil {
		logger.Println("Rados connect failed:", err)
	}
	logger.Println("Rados connect successily")

	// Get pool name
	poolName := mux.Vars(r)["name"]

	// list rbd image in pool
	images, err := handler.ListImage(poolName)
	if err != nil {
		logger.Println("List image in pool rbd failed:", err)
	}
	json.NewEncoder(w).Encode(images)
}

func GetRepos(w http.ResponseWriter, r *http.Request) {
	rh := repo.NewRepositoryHandler("192.168.15.100:6379")
	repos, err := rh.ListRepo()
	if err != nil {
		log.Println("List Repo failed:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(repos)
}

func CreateRepo(w http.ResponseWriter, r *http.Request) {

	repository := repo.Repository{}
	err := json.NewDecoder(r.Body).Decode(&repository)
	if err != nil {
		log.Println("Add Repo failed:", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	rh := repo.NewRepositoryHandler("192.168.15.100:6379")
	err = rh.AddRepo(repository)
	if err != nil {
		log.Println("Add Repo failed:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func DeleteRepo(w http.ResponseWriter, r *http.Request) {

	repository := repo.Repository{}
	err := json.NewDecoder(r.Body).Decode(&repository)
	if err != nil {
		log.Println("Delete Repo failed:", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	rh := repo.NewRepositoryHandler("192.168.15.100:6379")
	err = rh.AddRepo(repository)
	if err != nil {
		log.Println("Delete Repo failed:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func GetJobs(w http.ResponseWriter, r *http.Request) {
	jb := job.NewJobHandler("192.168.15.100:6379")
	jobs, err := jb.ListJob()
	if err != nil {
		log.Println(err)
	}
	json.NewEncoder(w).Encode(jobs)
}

func CreateJob(w http.ResponseWriter, r *http.Request) {
	task := job.Task{}
	err := json.NewDecoder(r.Body).Decode(&task)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}
	jb := job.NewJobHandler("192.168.15.100:6379")
	uuid, err := jb.CreateJob(task)
	if err != nil {
		http.Error(w, "Internal Server Error: can not operate redis server", http.StatusInternalServerError)
	}

	fn := func(progress int) {
		jb.UpdateJobProgress(uuid, progress)
	}

	ch := ceph.CephHandler{}
	switch task.Type {
	case "backup":
		err = ch.Backup(task.Pool, task.Image, task.Path, fn)
		if err != nil {
			http.Error(w, "Internal Server Error: backup progress is not executed", http.StatusInternalServerError)
		}
	case "restore":
		err = ch.Restore(task.Pool, task.Path, fn)
		if err != nil {
			http.Error(w, "Internal Server Error: backup progress is not executed", http.StatusInternalServerError)
		}
	}
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/pools", GetPools).Methods("GET")
	router.HandleFunc("/pools/{name}/images", GetImages).Methods("GET")
	router.HandleFunc("/repos", GetRepos).Methods("GET")
	router.HandleFunc("/repos", CreateRepo).Methods("POST")
	router.HandleFunc("/jobs", GetJobs).Methods("GET")
	router.HandleFunc("/jobs", CreateJob).Methods("POST")
	log.Fatal(http.ListenAndServe(":8000", router))

	/*logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	handler, err := NewCephHandler()
	if err != nil {
		logger.Println("Rados connect failed:", err)
	}
	logger.Println("Rados connect successily")

	err = handler.Restore("rbd", "/mnt/test.bk")
	if err != nil {
		logger.Println("restore failed:", err)
	}*/

	/*err = handler.Backup("rbd", "test", "/mnt/test.bk")
	if err != nil {
		logger.Println("backup failed:", err)
	}*/
}
