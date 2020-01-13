package storage

import (
	"fmt"
	"reflect"

	"github.com/yinyihanbing/gutils/logs"
)

// 从DB中加载数据存储到Redis中
func ReloadAllFormDbToRedis(dbCli *DbCli, redisCli *RedisCli, redisKey string, uniqueField string, p interface{}) (n int, ok bool) {
	// 从数据库加载
	err := dbCli.SelectMultiple(p, nil)
	if err != nil {
		logs.Error(err)
		return 0, false
	}

	// slice to map
	v := reflect.ValueOf(p)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n = v.Len()

	mData := make(map[interface{}]interface{}, n)
	for i := 0; i < n; i++ {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		objId := rv.Elem().FieldByName(uniqueField)
		if !objId.IsValid() {
			logs.Error(fmt.Sprintf("not exists field: k=%v, uniqueField=%v, err=%v", redisKey, uniqueField, err))
			return 0, false
		}
		mData[objId] = item
	}

	// 存储到Redis中
	err = redisCli.DoHMSetExt(redisKey, mData)
	if err != nil {
		return 0, false
	}

	return n, true
}

// 加载
func ReloadAllFormDbToRedisExt(dbCli *DbCli, redisCli *RedisCli, redisKeyPrefix string, uniqueKeyField, uniqueField string, prt interface{}) (n int, ok bool) {
	// 从数据库加载
	err := dbCli.SelectMultiple(prt, nil)
	if err != nil {
		logs.Error(err)
		return 0, false
	}

	// slice to map
	v := reflect.ValueOf(prt)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n = v.Len()

	mData := make(map[string]map[interface{}]interface{}, n)
	for i := 0; i < n; i++ {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		objKeyField := rv.Elem().FieldByName(uniqueKeyField)
		if !objKeyField.IsValid() {
			logs.Error(fmt.Sprintf("not exists field: k=%v, uniqueKeyField=%v, err=%v", redisKeyPrefix, uniqueKeyField, err))
			return 0, false
		}

		objField := rv.Elem().FieldByName(uniqueField)
		if !objField.IsValid() {
			logs.Error(fmt.Sprintf("not exists field: k=%v, uniqueField=%v, err=%v", redisKeyPrefix, uniqueField, err))
			return 0, false
		}

		rKey := fmt.Sprintf("%v_%v", redisKeyPrefix, objKeyField)
		rField := fmt.Sprintf("%v", objField)

		if _, ok := mData[rKey]; !ok {
			mData[rKey] = make(map[interface{}]interface{})
		}
		mData[rKey][rField] = item
	}

	// 存储到Redis中
	for k1, v1 := range mData {
		// 清空旧Redis缓存
		if err := GetRedisCli().DoDel(k1); err != nil {
			return 0, false
		}
		// 写入Redis缓存
		err = redisCli.DoHMSetExt(k1, v1)
		if err != nil {
			return 0, false
		}
	}

	return n, true
}

// 从DB中加载数据存储到Redis中
func ReloadMultipleFormDbToRedis(dbCli *DbCli, redisCli *RedisCli, redisKey string, uniqueField string, p interface{}, params map[string]interface{}) (n int, ok bool) {
	// 从数据库加载
	err := dbCli.SelectMultiple(p, params)
	if err != nil {
		logs.Error(err)
		return 0, false
	}

	// slice to map
	v := reflect.ValueOf(p)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n = v.Len()

	mData := make(map[interface{}]interface{}, n)
	for i := 0; i < n; i++ {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		objId := rv.Elem().FieldByName(uniqueField)
		if !objId.IsValid() {
			logs.Error(fmt.Sprintf("not exists field: k=%v, uniqueField=%v, err=%v", redisKey, uniqueField, err))
			return 0, false
		}
		mData[objId] = item
	}

	// 存储到Redis中
	err = redisCli.DoHMSetExt(redisKey, mData)
	if err != nil {
		return 0, false
	}

	return n, true
}

// 获取
func GetFromRedisByUniqueField(redisCli *RedisCli, redisKey string, uniqueFieldValues interface{}, prt interface{}) bool {
	v := reflect.ValueOf(uniqueFieldValues)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n := v.Len()
	args := make([]interface{}, n)
	for i := 0; i < n; i++ {
		args[i] = v.Index(i).Interface()
	}

	err := redisCli.DoHMGetExt(redisKey, prt, args...)
	if err != nil {
		return false
	}
	return true
}
