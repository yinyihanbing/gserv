package storage

import (
	"fmt"
	"reflect"

	"github.com/yinyihanbing/gutils/logs"
)

// reload all data from the database and store it in redis
func ReloadAllFormDbToRedis(dbCli *DbCli, redisCli *RedisCli, redisKey string, uniqueField string, p any) (n int, ok bool) {
	// load data from the database
	err := dbCli.SelectMultiple(p, nil)
	if err != nil {
		logs.Error(fmt.Sprintf("failed to load data from database: redis_key=%v, err=%v", redisKey, err))
		return 0, false
	}

	// convert slice to map
	v := reflect.ValueOf(p)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n = v.Len()

	mData := make(map[any]any, n)
	for i := range n {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		objId := rv.Elem().FieldByName(uniqueField)
		if !objId.IsValid() {
			logs.Error(fmt.Sprintf("field not found: redis_key=%v, unique_field=%v", redisKey, uniqueField))
			return 0, false
		}
		mData[objId] = item
	}

	// store data in redis
	err = redisCli.DoHMSet(redisKey, mData)
	if err != nil {
		logs.Error(fmt.Sprintf("failed to store data in redis: redis_key=%v, err=%v", redisKey, err))
		return 0, false
	}

	return n, true
}

// reload all data from the database and store it in redis with extended functionality
func ReloadAllFormDbToRedisExt(dbCli *DbCli, redisCli *RedisCli, redisKeyPrefix string, uniqueKeyField, uniqueField string, prt any) (n int, ok bool) {
	// load data from the database
	err := dbCli.SelectMultiple(prt, nil)
	if err != nil {
		logs.Error(fmt.Sprintf("failed to load data from database: redis_key_prefix=%v, err=%v", redisKeyPrefix, err))
		return 0, false
	}

	// convert slice to nested map
	v := reflect.ValueOf(prt)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n = v.Len()

	mData := make(map[string]map[any]any, n)
	for i := 0; i < n; i++ {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		objKeyField := rv.Elem().FieldByName(uniqueKeyField)
		if !objKeyField.IsValid() {
			logs.Error(fmt.Sprintf("field not found: redis_key_prefix=%v, unique_key_field=%v", redisKeyPrefix, uniqueKeyField))
			return 0, false
		}

		objField := rv.Elem().FieldByName(uniqueField)
		if !objField.IsValid() {
			logs.Error(fmt.Sprintf("field not found: redis_key_prefix=%v, unique_field=%v", redisKeyPrefix, uniqueField))
			return 0, false
		}

		rKey := fmt.Sprintf("%v_%v", redisKeyPrefix, objKeyField)
		rField := fmt.Sprintf("%v", objField)

		if _, ok := mData[rKey]; !ok {
			mData[rKey] = make(map[any]any)
		}
		mData[rKey][rField] = item
	}

	// store data in redis
	for k1, v1 := range mData {
		// clear old redis cache
		if err := GetRedisCli().DoDel(k1); err != nil {
			logs.Error(fmt.Sprintf("failed to delete old redis data: redis_key=%v, err=%v", k1, err))
			return 0, false
		}
		// write new data to redis
		err = redisCli.DoHMSet(k1, v1)
		if err != nil {
			logs.Error(fmt.Sprintf("failed to store data in redis: redis_key=%v, err=%v", k1, err))
			return 0, false
		}
	}

	return n, true
}

// reload data from the database with parameters and store it in redis
func ReloadMultipleFormDbToRedis(dbCli *DbCli, redisCli *RedisCli, redisKey string, uniqueField string, p any, params map[string]any) (n int, ok bool) {
	// load data from the database
	err := dbCli.SelectMultiple(p, params)
	if err != nil {
		logs.Error(fmt.Sprintf("failed to load data from database: redis_key=%v, err=%v", redisKey, err))
		return 0, false
	}

	// convert slice to map
	v := reflect.ValueOf(p)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n = v.Len()

	mData := make(map[any]any, n)
	for i := range n {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		objId := rv.Elem().FieldByName(uniqueField)
		if !objId.IsValid() {
			logs.Error(fmt.Sprintf("field not found: redis_key=%v, unique_field=%v", redisKey, uniqueField))
			return 0, false
		}
		mData[objId] = item
	}

	// store data in redis
	err = redisCli.DoHMSet(redisKey, mData)
	if err != nil {
		logs.Error(fmt.Sprintf("failed to store data in redis: redis_key=%v, err=%v", redisKey, err))
		return 0, false
	}

	return n, true
}

// retrieve data from redis by unique field values
func GetFromRedisByUniqueField(redisCli *RedisCli, redisKey string, uniqueFieldValues any, prt any) bool {
	v := reflect.ValueOf(uniqueFieldValues)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n := v.Len()
	args := make([]any, n)
	for i := range n {
		args[i] = v.Index(i).Interface()
	}

	// retrieve data from redis
	err := redisCli.DoHMGet(redisKey, prt, args...)
	if err != nil {
		logs.Error(fmt.Sprintf("failed to retrieve data from redis: redis_key=%v, err=%v", redisKey, err))
		return false
	}
	return true
}
