package redis

import (
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"log"
)

type RedisHandler struct {
	address string
}

func New(address string) *RedisHandler{
	return &RedisHandler{address}
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

func (h *RedisHandler) Add(i interface{}, namespace string, uuid string) error {
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	client, err := h.connect()
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Do("SET", namespace + "-" + uuid, string(b))
	if err != nil {
		return err
	}

	client.Do("RPUSH", namespace + "-list", uuid)
	return nil
}

func (h *RedisHandler) List(namespace string) ([]string, error) {
	client, err := h.connect()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	log.Println("Loading list from namespace",  namespace)
	vs, err := redis.Values(client.Do("LRANGE", namespace + "-list", 0, -1)) // take all element
	if err != nil {
		return nil, err
	}
	log.Println("Loading list from", namespace, "done")

	list := make([]string, 0)
	for _, v  := range vs {
		uuid := string(v.([]byte))
		v, err := redis.String(client.Do("GET", namespace + "-" + uuid))
		if err != nil {
			continue
		}
		list = append(list, v)
	}
	return list, nil
}


func (h *RedisHandler) UpdateProgress(namespace string, uuid string, percentage int) error {
	client, err := h.connect()
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Do("SET", namespace + "-" + uuid + "-progress", percentage)
	return err
}
