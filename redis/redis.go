package redis

import (
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"log"
)

type RedisHandler struct {
	address string
	namespace string
}

func New(address string, namespace string) *RedisHandler{
	return &RedisHandler{address, namespace}
}

func (h *RedisHandler) connect() (redis.Conn, error){
	log.Println("Connect redis server, address is", h.address)
	client, err := redis.Dial("tcp", h.address)
	if err != nil {
		return nil, err
	}
	log.Println("Redis server is connected")
	return client, nil
}

func (h *RedisHandler) Add(i interface{}, uuid string) error {
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	client, err := h.connect()
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Do("SET", h.namespace + "-" + uuid, string(b))
	if err != nil {
		return err
	}

	client.Do("RPUSH", h.namespace + "-list", uuid)
	return nil
}

func (h *RedisHandler) Load(uuid string) ([]byte, error) {
	client, err := h.connect()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return redis.Bytes(client.Do("GET", h.namespace + "-" + uuid))
}

func (h *RedisHandler) List() ([]string, error) {
	client, err := h.connect()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	log.Println("Loading list from namespace",  h.namespace)
	vs, err := redis.Values(client.Do("LRANGE", h.namespace + "-list", 0, -1)) // take all element
	if err != nil {
		return nil, err
	}
	log.Println("Loading list from", h.namespace, "done")

	list := make([]string, 0)
	for _, v  := range vs {
		uuid := string(v.([]byte))
		v, err := redis.String(client.Do("GET", h.namespace + "-" + uuid))
		if err != nil {
			continue
		}
		list = append(list, v)
	}
	return list, nil
}

func (h *RedisHandler) Delete(uuid string) error {
	client, err := h.connect()
	if err != nil {
		return err
	}
	defer client.Close()

	log.Println("Removing element", uuid, "from namespace",  h.namespace)

	element := h.namespace + "-" + uuid
	_, err = client.Do("DEL", element, element + "-progress")
	if err != nil {
		return err
	}
	_, err = client.Do("LREM", h.namespace + "-list", 0, uuid)
	return err
}

func (h *RedisHandler) IsExists(uuid string) (bool, error) {
	client, err := h.connect()
	if err != nil {
		return false, err
	}
	defer client.Close()

	key := h.namespace + "-" + uuid
    ok, err := redis.Bool(client.Do("EXISTS", key))
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (h *RedisHandler) GetProgress(uuid string) (string, error) {
	client, err := h.connect()
	if err != nil {
		return "", err
	}
	defer client.Close()

	return redis.String(client.Do("GET", h.namespace + "-" + uuid + "-progress"))
}

func (h *RedisHandler) UpdateProgress(uuid string, percentage int) error {
	client, err := h.connect()
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Do("SET", h.namespace + "-" + uuid + "-progress", percentage)
	return err
}
