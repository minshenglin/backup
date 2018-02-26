package main

import (
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"github.com/gorilla/mux"
	"log"
	"os"
	"os/exec"
	"net/http"
	"encoding/json"
	"time"
	//"regexp"
	//"strings"
)

type Pool struct {
	Name string  `json:"name"`
	Usage uint64 `json:"usage"` //unit: byte
}

type Image struct {
	Name string `json:"name"`
	Size uint64 `json:"size"` //unit: byte
}

type CephHandler struct {
	conn *rados.Conn
}

func NewCephHandler() (*CephHandler, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, err
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		return nil, err
	}

	err = conn.Connect()
	if err != nil {
		return nil, err
	}

	return &CephHandler{conn}, nil
}

func (ch *CephHandler) ListPool() ([]Pool, error) {
	poolNames, err := ch.conn.ListPools()
	if err != nil {
		return nil, err
	}

	pools := make([]Pool, 0)
	for _, name := range poolNames {
		ioctx, err := ch.conn.OpenIOContext(name)
		if err != nil {
			continue
		}
		defer ioctx.Destroy()

		stat, err := ioctx.GetPoolStats()
		if err != nil {
			continue
		}
		usage := stat.Num_bytes
		//pools[index] = Pool{name, usage}
		pools = append(pools, Pool{name, usage})
	}
	return pools, nil
}

func (ch *CephHandler) LoadImage(pool string, name string) (*Image, error){
	ioctx, err := ch.conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}
	defer ioctx.Destroy()

	img := rbd.GetImage(ioctx, name)
	if err := img.Open(true); err != nil {
		return nil, err
	}
	defer img.Close()
	size, err := img.GetSize()
	if err != nil {
		return nil, err
	}
	return &Image{name, size}, nil
}

func (ch *CephHandler) ListImage(pool string) ([]Image, error) {
	ioctx, err := ch.conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}
	defer ioctx.Destroy()

	imgNames, err := rbd.GetImageNames(ioctx)
	if err != nil {
		return nil, err
	}

	images := make([]Image, 0)

	for _, name := range imgNames {
		img := rbd.GetImage(ioctx, name)
		err := img.Open(true)
		if err != nil {
			continue
		}
		defer img.Close()
		size, err := img.GetSize()
		if err != nil {
			continue
		}
		images = append(images, Image{name, size})
	}
	return images, nil
}

func (ch *CephHandler) Backup(pool string, img string, path string) error {
	cmd := exec.Command("/usr/bin/rbd", "export", "--pool", pool, img, path)
	if err := cmd.Start(); err != nil {
		return err
	}

	instance, err := ch.LoadImage("rbd", "test")
	if err != nil {
		log.Println("Loading image failed:", err)
	}
	totalSize := int64(instance.Size)
	percentage := 0

	stop := make(chan bool)
	go func() {
		tick := time.Tick(100 * time.Millisecond)
		for {
			select {
			case <- tick:
				stat, err := os.Stat(path)
				if err != nil {
					continue
				}
				percentage = int(float64(stat.Size())/float64(totalSize) * 100)
				log.Println("Backup Progress:", percentage, "%")
			case <- stop:
				return
			}
		}
	}()

	err = cmd.Wait()
	stop <- true
	return err
}

func (ch *CephHandler) Restore(pool string, path string) error {
	cmd := exec.Command("/usr/bin/rbd", "import", "--dest-pool", pool, path)
	err := cmd.Run()
	return err
}

func GetPools(w http.ResponseWriter, r *http.Request) {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	// connect to ceph cluster
	handler, err := NewCephHandler()
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
	handler, err := NewCephHandler()
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

func main() {
	/*router := mux.NewRouter()
	router.HandleFunc("/pools", GetPools).Methods("GET")
	router.HandleFunc("/pools/{name}/images", GetImages).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))*/
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	handler, err := NewCephHandler()
	if err != nil {
		logger.Println("Rados connect failed:", err)
	}
	logger.Println("Rados connect successily")
	/*
	err = handler.Restore("rbd", "/mnt/test.bk")
	if err != nil {
		logger.Println("restore failed:", err)
	}*/

	err = handler.Backup("rbd", "test", "/mnt/test.bk")
	if err != nil {
		logger.Println("backup failed:", err)
	}
}

