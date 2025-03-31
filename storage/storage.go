package storage

import (
	"errors"
	"fmt"
)

var storage *Storage

type Storage struct {
	redisClis map[int]*RedisCli
	dbClis    map[int]*DbCli
}

// initialize storage with empty redis and db clients
func init() {
	storage = &Storage{}
	storage.redisClis = map[int]*RedisCli{}
	storage.dbClis = map[int]*DbCli{}
}

// get redis client with index 0
func GetRedisCli() *RedisCli {
	return storage.redisClis[0]
}

// get redis client by specific index
func GetRedisCliExt(idx int) *RedisCli {
	return storage.redisClis[idx]
}

// get db client with index 0
func GetDbCli() *DbCli {
	return storage.dbClis[0]
}

// get db client by specific index
func GetDbCliExt(idx int) *DbCli {
	return storage.dbClis[idx]
}

// release all resources
func Destroy() {
	storage.Destroy()
}

// add a redis client with a specific index
func AddRedisCli(redisCliIdx int, redisCfg *RedisConfig) error {
	if _, ok := storage.redisClis[redisCliIdx]; ok {
		return fmt.Errorf("redis client with index %v already exists", redisCliIdx)
	}

	redisCli, err := newRedisClipool(redisCfg)
	if err != nil {
		return err
	}
	storage.redisClis[redisCliIdx] = redisCli

	return nil
}

// add a db client with a specific index
func AddDbCli(dbCliIdx int, dbCfg *DbConfig) error {
	if _, ok := storage.dbClis[dbCliIdx]; ok {
		return fmt.Errorf("db client with index %v already exists", dbCliIdx)
	}

	dbCli, err := newDbCli(dbCfg)
	if err != nil {
		return err
	}
	storage.dbClis[dbCliIdx] = dbCli

	return nil
}

// start persistence queues for all db clients
func StartDbQueue() {
	for _, v := range storage.dbClis {
		v.StartQueue()
	}
}

// start persistence queue for a specific db client
func StartDbQueueExt(dbCliIdx int) {
	storage.dbClis[dbCliIdx].StartQueue()
}

// get the total number of tasks in all db queues
func GetDbQueueTaskCount() (count int64) {
	for _, v := range storage.dbClis {
		if v.dbQueue != nil {
			count += v.dbQueue.GetQueueCount()
		}
	}
	return count
}

// release all resources for storage
func (s *Storage) Destroy() {
	for _, dbCli := range s.dbClis {
		dbCli.Destroy()
	}
	for _, redisCli := range s.redisClis {
		redisCli.Destroy()
	}
}

// add data to both redis and db
func Add(key string, field any, p any) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("add: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoHSet(key, field, p)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("add: db client with index 0 does not exist")
		return
	}
	dbCli.AsyncInsert(p)

	return nil
}

// delete data from both redis and db
func Delete(key string, field any, p any) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("delete: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoHDel(key, field)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("delete: db client with index 0 does not exist")
		return
	}
	dbCli.AsyncDelete(p)

	return nil
}

// update data in both redis and db
func Update(key string, field any, p any, fields ...string) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("update: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoHSet(key, field, p)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("update: db client with index 0 does not exist")
		return
	}
	dbCli.AsyncUpdate(p, fields...)

	return nil
}

// update multiple fields in both redis and db
func UpdateMultiple(key string, args map[any]any, fields ...string) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("update multiple: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoHMSet(key, args)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("update multiple: db client with index 0 does not exist")
		return
	}
	for _, v := range args {
		dbCli.AsyncUpdate(v, fields...)
	}

	return nil
}

// add multiple entries to both redis and db
func AddMultiple(key string, args map[any]any) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("add multiple: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoHMSet(key, args)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("add multiple: db client with index 0 does not exist")
		return
	}
	for _, v := range args {
		dbCli.AsyncInsert(v)
	}

	return nil
}

// delete multiple entries from both redis and db
func DeleteMultiple(key string, args map[any]any) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("delete multiple: redis client with index 0 does not exist")
		return
	}

	delIds := make([]any, 0, len(args))
	for k := range args {
		delIds = append(delIds, k)
	}

	err = redisCli.DoHDel(key, delIds...)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("delete multiple: db client with index 0 does not exist")
		return
	}
	for _, v := range args {
		dbCli.AsyncDelete(v)
	}

	return nil
}

// add data to redis sorted set and db
func ZAdd(key string, score int64, member any, p any) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("zadd: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoZAdd(key, score, member)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("zadd: db client with index 0 does not exist")
		return
	}
	dbCli.AsyncInsert(p)

	return nil
}

// update data in redis sorted set and db
func ZUpdate(key string, score int64, member any, p any, fields ...string) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New("zupdate: redis client with index 0 does not exist")
		return
	}

	err = redisCli.DoZAdd(key, score, member)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New("zupdate: db client with index 0 does not exist")
		return
	}
	dbCli.AsyncUpdate(p, fields...)

	return nil
}
