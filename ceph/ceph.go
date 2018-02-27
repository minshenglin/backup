package ceph

import (
	"bufio"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"time"
)

type Pool struct {
	Name  string `json:"name"`
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

func (ch *CephHandler) LoadImage(pool string, name string) (*Image, error) {
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

	info, err := img.Stat()
	if err != nil {
		return nil, err
	}
	return &Image{name, info.Size}, nil
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
		info, err := img.Stat()
		if err != nil {
			return nil, err
		}
		images = append(images, Image{name, info.Size})
	}
	return images, nil
}

func (ch *CephHandler) progressCommand(command []string, fn func(int)) error {
	cmd := exec.Command(command[0], command[1:]...)
	stderr, err := cmd.StderrPipe() // ceph rbd command use stderr to print progress
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	stop := make(chan bool)

	go func() {
		tick := time.Tick(100 * time.Millisecond)
		reader := bufio.NewReader(stderr)
		re := regexp.MustCompile("[0-9]+")
		percent := 0
		for {
			select {
			case <-tick:
				buffer := make([]byte, 1024)
				n, err := reader.Read(buffer)
				if err != nil {
					continue
				}
				i, err := strconv.Atoi(re.FindString(string(buffer[:n])))
				if err == nil && i > percent {
					percent = i
					fn(percent)
				}
			case <-stop:
				if percent < 100 {
					fn(100) // make sure percentage is 100 when done
				}
				return
			}
		}
	}()
	err = cmd.Wait()
	stop <- true
	return err
}

func (ch *CephHandler) Backup(pool string, img string, path string) error {
	command := []string{"/usr/bin/rbd", "export", "--pool", pool, img, path}
	fn := func(i int) {
		log.Println("Backup Progress: ", i, "%")
	}
	err := ch.progressCommand(command, fn)
	return err
}

func (ch *CephHandler) Restore(pool string, path string) error {
	command := []string{"/usr/bin/rbd", "import", "--dest-pool", pool, path}
	fn := func(i int) {
		log.Println("Restore Progress: ", i, "%")
	}
	err := ch.progressCommand(command, fn)
	return err
}

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	handler, err := NewCephHandler()
	if err != nil {
		logger.Println("Rados connect failed:", err)
	}
	logger.Println("Rados connect successily")

	err = handler.Restore("rbd", "/mnt/test.bk")
	if err != nil {
		logger.Println("restore failed:", err)
	}

	/*err = handler.Backup("rbd", "test", "/mnt/test.bk")
	if err != nil {
		logger.Println("backup failed:", err)
	}*/
}
