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
	_, err = rh.AddRepo(&repository)
	if err != nil {
		log.Println("Add Repo failed:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(repository)
}

func DeleteRepo(w http.ResponseWriter, r *http.Request) {
	rh := repo.NewRepositoryHandler("192.168.15.100:6379")
	// Get pool name
	uuid := mux.Vars(r)["uuid"]
	err := rh.RemoveRepo(uuid)
	if err != nil {
		log.Println("Delete Repo failed:", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
}

func GetJobs(w http.ResponseWriter, r *http.Request) {
	jh := job.NewJobHandler("192.168.15.100:6379")
	jobs, err := jh.ListJob()
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
		return
	}

	rh := repo.NewRepositoryHandler("192.168.15.100:6379")
	repository, err := rh.LoadRepo(task.RepoUuid)
	if err != nil {
		log.Println("Loading repo", task.RepoUuid, "failed", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	jh := job.NewJobHandler("192.168.15.100:6379")
	job, err := jh.CreateJob(task)
	if err != nil {
		http.Error(w, "Internal Server Error: can not operate redis server", http.StatusInternalServerError)
		return
	}

	fn := func(progress int) {
		jh.UpdateJobProgress(job.Uuid, progress)
	}

	ch := ceph.CephHandler{}
	switch task.Type {
	case "backup":
		err = ch.Backup(task.Pool, task.Image, repository.Path + "/"  + task.Image, fn)
		if err != nil {
			http.Error(w, "Internal Server Error: backup progress is not executed", http.StatusInternalServerError)
			return
		}
	case "restore":
		err = ch.Restore(task.Pool, repository.Path + "/" + task.Image, fn)
		if err != nil {
			http.Error(w, "Internal Server Error: backup progress is not executed", http.StatusInternalServerError)
			return
		}
	}
	json.NewEncoder(w).Encode(job)
}

func GetJobProgress(w http.ResponseWriter, r *http.Request) {
	jh := job.NewJobHandler("192.168.15.100:6379")
	// Get job uuid
	uuid := mux.Vars(r)["uuid"]
	progress, err := jh.GetJobProgress(uuid)
	if err != nil {
		log.Println("Get the progress of repo", uuid, "failed:", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	w.Write([]byte(progress))
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/pools", GetPools).Methods("GET")
	router.HandleFunc("/pools/{name}/images", GetImages).Methods("GET")
	router.HandleFunc("/repos", GetRepos).Methods("GET")
	router.HandleFunc("/repos", CreateRepo).Methods("POST")
	router.HandleFunc("/repos/{uuid}", DeleteRepo).Methods("DELETE")
	router.HandleFunc("/jobs", GetJobs).Methods("GET")
	router.HandleFunc("/jobs", CreateJob).Methods("POST")
	router.HandleFunc("/jobs/{uuid}/progress", GetJobProgress).Methods("GET")
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
