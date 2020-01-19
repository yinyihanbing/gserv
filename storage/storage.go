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

func init() {
	storage = &Storage{}
	storage.redisClis = map[int]*RedisCli{}
	storage.dbClis = map[int]*DbCli{}
}

// 获取编号为0的Redis连接
func GetRedisCli() *RedisCli {
	return storage.redisClis[0]
}

// 获取指定编号的Redis连接
func GetRedisCliExt(idx int) *RedisCli {
	return storage.redisClis[idx]
}

// 获取编号为0的Db连接
func GetDbCli() *DbCli {
	return storage.dbClis[0]
}

// 获取指定编号的Db连接
func GetDbCliExt(idx int) *DbCli {
	return storage.dbClis[idx]
}

// 释放资源
func Destroy() {
	storage.Destroy()
}

// 添加Redis连接
func AddRedisCli(redisCliIdx int, redisCfg *RedisConfig) error {
	if _, ok := storage.redisClis[redisCliIdx]; ok {
		return errors.New(fmt.Sprintf("RedisCli idx %v has already existed", redisCliIdx))
	}

	redisCli, err := newRedisClipool(redisCfg)
	if err != nil {
		return err
	}
	storage.redisClis[redisCliIdx] = redisCli

	return nil
}

// 添加Db连接
func AddDbCli(dbCliIdx int, dbCfg *DbConfig) error {
	if _, ok := storage.dbClis[dbCliIdx]; ok {
		return errors.New(fmt.Sprintf("DbCli idx %v has already existed", dbCliIdx))
	}

	dbCli, err := newDbCli(dbCfg)
	if err != nil {
		return err
	}
	storage.dbClis[dbCliIdx] = dbCli

	return nil
}

// 启动数据库持久化队列
func StartDbQueue() {
	for _, v := range storage.dbClis {
		v.StartQueue()
	}
}

// 启动指定数据库持久化队列
func StartDbQueueExt(dbCliIdx int) {
	storage.dbClis[dbCliIdx].StartQueue()
}

// 当前DB队列带执行任务数量
func GetDbQueueTaskCount() (count int64) {
	for _, v := range storage.dbClis {
		if v.dbQueue != nil {
			count += v.dbQueue.GetQueueCount()
		}
	}
	return count
}

// 释放资源
func (s *Storage) Destroy() {
	for _, dbCli := range s.dbClis {
		dbCli.Destroy()
	}
	for _, redisCli := range s.redisClis {
		redisCli.Destroy()
	}
}

// 从Redis和Db中添加数据
func Add(key string, field interface{}, p interface{}) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("Add: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoHSet(key, field, p)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("Add: dbCli idx %v not exists", 0))
		return
	}
	dbCli.AsyncInsert(p)

	return nil
}

// 从Redis和Db中删除数据
func Delete(key string, field interface{}, p interface{}) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("Delete: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoHDel(key, field)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("Delete: dbCli idx %v not exists", 0))
		return
	}
	dbCli.AsyncDelete(p)

	return nil
}

// 从Redis和Db中更新数据
func Update(key string, field interface{}, p interface{}, fields ... string) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("Update: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoHSet(key, field, p)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("Update: dbCli idx %v not exists", 0))
		return
	}
	dbCli.AsyncUpdate(p, fields...)

	return nil
}

// 从Redis和Db中更新数据
func UpdateMultiple(key string, args map[interface{}]interface{}, fields ... string) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("UpdateMultiple: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoHMSet(key, args)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("UpdateMultiple: dbCli idx %v not exists", 0))
		return
	}
	for _, v := range args {
		dbCli.AsyncUpdate(v, fields...)
	}

	return nil
}

// 从Redis和Db中添加数据
func AddMultiple(key string, args map[interface{}]interface{}) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("AddMultiple: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoHMSet(key, args)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("AddMultiple: dbCli idx %v not exists", 0))
		return
	}
	for _, v := range args {
		dbCli.AsyncInsert(v)
	}

	return nil
}

// 从Redis和Db中删除数据
func DeleteMultiple(key string, args map[interface{}]interface{}) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("DeleteMultiple: redisCli idx %v not exists", 0))
		return
	}

	delIds := make([]interface{}, 0, len(args))
	for k := range args {
		delIds = append(delIds, k)
	}

	err = redisCli.DoHDel(key, delIds...)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("DeleteMultiple: dbCli idx %v not exists", 0))
		return
	}
	for _, v := range args {
		dbCli.AsyncDelete(v)
	}

	return nil
}

// 从Redis和Db中添加数据
func ZAdd(key string, score int64, member interface{}, p interface{}) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("ZAdd: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoZAdd(key, score, member)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("ZAdd: dbCli idx %v not exists", 0))
		return
	}
	dbCli.AsyncInsert(p)

	return nil
}

// 从Redis和Db中更新数据
func ZUpdate(key string, score int64, member interface{}, p interface{}, fields ... string) (err error) {
	redisCli := GetRedisCli()
	if redisCli == nil {
		err = errors.New(fmt.Sprintf("ZUpdate: redisCli idx %v not exists", 0))
		return
	}

	err = redisCli.DoZAdd(key, score, member)
	if err != nil {
		return
	}

	dbCli := GetDbCli()
	if dbCli == nil {
		err = errors.New(fmt.Sprintf("Update: dbCli idx %v not exists", 0))
		return
	}
	dbCli.AsyncUpdate(p, fields...)

	return nil
}
