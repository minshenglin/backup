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

func GetRepoInfo(w http.ResponseWriter, r *http.Request) {
	repo, err := repo.NewRepository("/mnt")
	if err != nil {
		log.Println("Loading repository info failed")
		return
	}
	json.NewEncoder(w).Encode(repo)
}
func main() {
	/*router := mux.NewRouter()
	router.HandleFunc("/pools", GetPools).Methods("GET")
	router.HandleFunc("/pools/{name}/images", GetImages).Methods("GET")
	router.HandleFunc("/repo", GetRepoInfo).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))*/

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
	jb := job.NewJobHandler("192.168.15.100:6379")
	uuid, err := jb.CreateJob("backup", "rbd", "test", "/mnt")
	if err != nil {
		log.Println(err)
	}
	log.Println(uuid)

	s, _ := jb.LoadJob(uuid)
	log.Println(s)
}
