package ceph

import (
	"bufio"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"os/exec"
	"regexp"
	"strconv"
	"time"
	"log"
)

type Pool struct {
	Name  string `json:"name"`
	Usage uint64 `json:"usage"` //unit: byte
}

type Image struct {
	Name string `json:"name"`
	Size uint64 `json:"size"` //unit: byte
}

type SnapShot struct {
	Timestamp   int `json:"timestamp"`
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

func (ch *CephHandler) ListSnapshot(pool string, imgName string) ([]SnapShot, error){
	ioctx, err := ch.conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}
	defer ioctx.Destroy()

	img := rbd.GetImage(ioctx, imgName)
	if err := img.Open(true); err != nil {
		return nil, err
	}
	defer img.Close()

	snaps := make([]SnapShot, 0)
	infos, err := img.GetSnapshotNames()
	if err != nil {
		return []SnapShot{}, err
	}

	for _, info := range infos {
		timestamp, err := strconv.Atoi(info.Name)
		if err != nil {
			continue
		}
		s := SnapShot{timestamp}
		snaps = append(snaps, s)
	}
	return snaps, nil
}

func (ch *CephHandler) CreateSnapshot(pool string, imgName string) error {
	ioctx, err := ch.conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()

	img := rbd.GetImage(ioctx, imgName)
	if err := img.Open(); err != nil {
		return err
	}
	defer img.Close()

	timestamp := time.Now().Unix()
	name := strconv.Itoa(int(timestamp))
	_, err = img.CreateSnapshot(name)
	return err
}

func (ch *CephHandler) RemoveSnapshot(pool string, imgName string, name string) error {
	ioctx, err := ch.conn.OpenIOContext(pool)
	if err != nil {
		return err
	}
	defer ioctx.Destroy()

	img := rbd.GetImage(ioctx, imgName)
	if err := img.Open(); err != nil {
		return err
	}
	defer img.Close()

	snapshot := img.GetSnapshot(name)
	return snapshot.Remove()
}

func (ch *CephHandler) progressCommand(command []string, fn func(int)) error {
	cmd := exec.Command(command[0], command[1:]...)
	stderr, err := cmd.StderrPipe() // ceph rbd command use stderr to print progress
	if err != nil {
		log.Println("Open stderr pipe failed")
		return err
	}
	if err := cmd.Start(); err != nil {
		log.Println("Execute command failed", command)
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
				return
			}
		}
	}()

	go func() {
		cmd.Wait()
		stop <- true
		fn(100) // make sure percentage is 100 when done
	}()

	return nil
}

func (ch *CephHandler) Backup(pool string, img string, path string, fn func(int)) error {
	command := []string{"/usr/bin/rbd", "export", "--pool", pool, img, path}
	err := ch.progressCommand(command, fn)
	return err
}

func (ch *CephHandler) Restore(pool string, path string, fn func(int)) error {
	command := []string{"/usr/bin/rbd", "import", "--dest-pool", pool, path}
	err := ch.progressCommand(command, fn)
	return err
}

func (ch *CephHandler) IncrementalBackup(pool string, img string, path string, start string, end string, fn func(int)) error {
	target := img + "@" + end
	command := []string{"/usr/bin/rbd", "export-diff", "--pool", pool, target, "--from-snap", start, path}
	err := ch.progressCommand(command, fn)
	return err
}
