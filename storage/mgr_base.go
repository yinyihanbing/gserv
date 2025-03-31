package storage

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/yinyihanbing/gutils/logs"
)

type MgrBase struct {
	baseRedisKey string
	ksName       []string
	fsName       []string
}

// set redis key prefix, key field names, and field names
func (mb *MgrBase) SetRedisKeyField(baseRedisKey string, ksName, fsName []string) {
	mb.baseRedisKey = baseRedisKey
	mb.ksName = ksName
	mb.fsName = fsName
}

// get redis key with values
func (mb *MgrBase) GetRedisKeyWithVal(kvs ...any) (string, error) {
	if len(kvs) != len(mb.ksName) {
		err := fmt.Errorf("cache:%v, key field count mismatch, expected fields:%v, provided values:%v", mb.baseRedisKey, mb.ksName, kvs)
		logs.Error(err)
		return "", err
	}

	rKey := mb.baseRedisKey
	for _, v := range kvs {
		rKey = fmt.Sprintf("%v_%v", rKey, v)
	}
	return rKey, nil
}

// get redis field with values
func (mb *MgrBase) GetRedisFieldWithVal(fvs ...any) (string, error) {
	if len(fvs) != len(mb.fsName) {
		err := fmt.Errorf("cache:%v, field count mismatch, expected fields:%v, provided values:%v", mb.baseRedisKey, mb.fsName, fvs)
		logs.Error(err)
		return "", err
	}

	rField := ""
	for _, v := range fvs {
		rField = fmt.Sprintf("%v_%v", rField, v)
	}
	rField = strings.TrimLeft(rField, "_")
	return rField, nil
}

// get redis key based on struct object
func (mb *MgrBase) GetRedisKeyWithObj(p any) (string, error) {
	if len(mb.baseRedisKey) == 0 && len(mb.ksName) == 0 {
		err := fmt.Errorf("cache:%v, base key or key field names not set", mb.baseRedisKey)
		logs.Error(err)
		return "", err
	}

	if len(mb.ksName) == 0 {
		return mb.baseRedisKey, nil
	}

	pv := reflect.ValueOf(p)
	if pv.Kind() == reflect.Ptr {
		pv = pv.Elem()
	}

	kvs := make([]any, 0, len(mb.ksName))
	for _, v := range mb.ksName {
		objKeyField := pv.FieldByName(v)
		if !objKeyField.IsValid() {
			err := fmt.Errorf("cache:%v, failed to read struct field:%v for redis key", mb.baseRedisKey, v)
			logs.Error(err)
			return "", err
		}
		kvs = append(kvs, objKeyField)
	}

	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	return rKey, err
}

// get redis field based on struct object
func (mb *MgrBase) GetRedisFieldWithObj(p any) (string, error) {
	if len(mb.fsName) == 0 {
		err := fmt.Errorf("cache:%v, field names not set", mb.baseRedisKey)
		logs.Error(err)
		return "", err
	}

	pv := reflect.ValueOf(p)
	if pv.Kind() == reflect.Ptr {
		pv = pv.Elem()
	}

	fvs := make([]any, 0, len(mb.fsName))
	for _, v := range mb.fsName {
		objKeyField := pv.FieldByName(v)
		if !objKeyField.IsValid() {
			err := fmt.Errorf("cache:%v, failed to read struct field:%v for redis field", mb.baseRedisKey, v)
			logs.Error(err)
			return "", err
		}
		fvs = append(fvs, objKeyField)
	}

	rField, err := mb.GetRedisFieldWithVal(fvs...)
	return rField, err
}

// get redis key and field based on struct object
func (mb *MgrBase) GetRedisKeyFieldWithObj(p any) (rKey string, rField string, err error) {
	rKey, err = mb.GetRedisKeyWithObj(p)
	if err != nil {
		return "", "", err
	}

	rField, err = mb.GetRedisFieldWithObj(p)
	if err != nil {
		return "", "", err
	}

	return
}

// convert slice to redis map format
func (mb *MgrBase) sliceToRedisMap(pSlice any) (mData map[string]map[any]any, err error) {
	v := reflect.ValueOf(pSlice)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	mData = make(map[string]map[any]any) // key1=redisKey, key2=redisField
	for i := range v.Len() {
		item := v.Index(i).Interface()

		rKey, err := mb.GetRedisKeyWithObj(item)
		if err != nil {
			return nil, err
		}

		rField, err := mb.GetRedisFieldWithObj(item)
		if err != nil {
			return nil, err
		}

		if _, ok := mData[rKey]; !ok {
			mData[rKey] = make(map[any]any)
		}
		mData[rKey][rField] = item
	}

	return mData, nil
}

// reload db data to redis, pSlice=container slice reference, dbQueryParams=database query parameters
func (mb *MgrBase) ReloadDbDataToRedis(pSlice any, dbQueryParams map[string]any) (n int, err error) {
	err = GetDbCli().SelectMultiple(pSlice, dbQueryParams)
	if err != nil {
		return 0, err
	}

	mData, err := mb.sliceToRedisMap(pSlice)
	if err != nil {
		return 0, err
	}

	for k1, v1 := range mData {
		err = GetRedisCli().DoHMSet(k1, v1)
		if err != nil {
			return 0, err
		}

		n += len(v1)
	}

	return n, nil
}

// db and redis add data
func (mb *MgrBase) Add(p ...any) bool {
	if len(p) == 0 {
		logs.Error("cache:%v, add empty", mb.baseRedisKey)
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := mb.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		err = Add(rKey, rField, p[0])
		if err != nil {
			logs.Error(fmt.Sprintf("db and redis add data failed, struct=%v, err=%v", reflect.TypeOf(p[0]), err))
			return false
		}
	} else {
		mData, err := mb.sliceToRedisMap(p)
		if err != nil {
			return false
		}
		for k, v := range mData {
			if err := AddMultiple(k, v); err != nil {
				return false
			}
		}
	}

	return true
}

// db and redis delete data
func (mb *MgrBase) Delete(p ...any) bool {
	if len(p) == 0 {
		logs.Error("cache:%v, delete empty", mb.baseRedisKey)
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := mb.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		err = Delete(rKey, rField, p[0])
		if err != nil {
			logs.Error(fmt.Sprintf("cache:%v, db and redis delete data failed, struct=%v, err=%v", mb.baseRedisKey, reflect.TypeOf(p[0]), err))
			return false
		}
	} else {
		mData, err := mb.sliceToRedisMap(p)
		if err != nil {
			return false
		}
		for k, v := range mData {
			if err := DeleteMultiple(k, v); err != nil {
				return false
			}
		}
	}

	return true
}

// db and redis update data
func (mb *MgrBase) Update(columns []string, p ...any) bool {
	if len(p) == 0 {
		logs.Error(fmt.Sprintf("cache:%v, update empty", mb.baseRedisKey))
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := mb.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		err = Update(rKey, rField, p[0], columns...)
		if err != nil {
			logs.Error(fmt.Sprintf("cache:%v, db and redis update data failed, struct=%v, err=%v", mb.baseRedisKey, reflect.TypeOf(p[0]), err))
			return false
		}
	} else {
		mData, err := mb.sliceToRedisMap(p)
		if err != nil {
			return false
		}
		for k, v := range mData {
			if err := UpdateMultiple(k, v, columns...); err != nil {
				return false
			}
		}
	}

	return true
}

// get single field data from redis
func (mb *MgrBase) GetWithSingleKVs(kv any, fv any, p any) (exists bool, err error) {
	var kvs []any = nil
	if kv != nil {
		kvs = []any{kv}
	}
	var fvs []any = nil
	if fv != nil {
		fvs = []any{fv}
	}
	return mb.Get(kvs, fvs, p)
}

// get single field data from redis
func (mb *MgrBase) Get(kvs []any, fvs []any, p any) (exists bool, err error) {
	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return false, err
	}

	rField, err := mb.GetRedisFieldWithVal(fvs...)
	if err != nil {
		return false, err
	}

	exists, err = GetRedisCli().DoHGetProto(rKey, rField, p)
	return
}

// get multiple field data from redis
func (mb *MgrBase) GetMultipleWithSingleKVs(kv any, fv []any, p any) error {
	fvs := make([][]any, 0, len(fv))
	for _, v := range fv {
		fvs = append(fvs, []any{v})
	}
	var kvs []any = nil
	if kv != nil {
		kvs = []any{kv}
	}
	return mb.GetMultiple(kvs, fvs, p)
}

// get multiple field data from redis
func (mb *MgrBase) GetMultiple(kvs []any, fvs [][]any, p any) error {
	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return err
	}

	args := make([]any, 0)
	for _, v := range fvs {
		rField, err := mb.GetRedisFieldWithVal(v...)
		if err != nil {
			return err
		}
		args = append(args, rField)
	}

	err = GetRedisCli().DoHMGet(rKey, p, args...)
	if err != nil {
		return err
	}

	return nil
}

// get all field data from redis, [0,n-1] are redis key values, [n-1] is the container reference
func (mb *MgrBase) GetAll(param ...any) error {
	if len(param) < 1 {
		err := fmt.Errorf("cache:%v, incorrect number of parameters:%v", mb.baseRedisKey, param)
		logs.Error(err)
		return err
	}
	if reflect.TypeOf(param[len(param)-1]).Kind() != reflect.Ptr {
		err := fmt.Errorf("cache:%v, incorrect parameter type:%v", mb.baseRedisKey, param)
		logs.Error(err)
		return err
	}

	rKey, err := mb.GetRedisKeyWithVal(param[:len(param)-1]...)
	if err != nil {
		return err
	}

	if err := GetRedisCli().DoHVals(rKey, param[len(param)-1]); err != nil {
		return err
	}

	return nil
}

// get field list length for single key
func (mb *MgrBase) GetFieldsLen(kvs ...any) (int64, error) {
	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return 0, err
	}

	l, err := GetRedisCli().DoHLen(rKey)
	if err != nil {
		return 0, err
	}

	return l, nil
}

// get field list, [0,n-1] are redis key values, [n-1] is the container reference
func (mb *MgrBase) GetFields(param ...any) error {
	if len(param) < 1 {
		err := fmt.Errorf("cache:%v, incorrect number of parameters:%v", mb.baseRedisKey, param)
		logs.Error(err)
		return err
	}
	if reflect.TypeOf(param[len(param)-1]).Kind() != reflect.Ptr {
		err := fmt.Errorf("cache:%v, incorrect parameter type:%v", mb.baseRedisKey, param)
		logs.Error(err)
		return err
	}

	rKey, err := mb.GetRedisKeyWithVal(param[:len(param)-1]...)
	if err != nil {
		return err
	}

	if err := GetRedisCli().DoHKeys(rKey, param[len(param)-1]); err != nil {
		return err
	}

	return nil
}

// check if key exists
func (mb *MgrBase) IsKeyExists(kvs ...any) (exists bool, err error) {
	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return false, err
	}

	exists, err = GetRedisCli().DoExists(rKey)
	return exists, err
}

// check if field exists
func (mb *MgrBase) IsFieldExists(kvs []any, fvs []any) (exists bool, err error) {
	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return false, err
	}

	rField, err := mb.GetRedisFieldWithVal(fvs...)
	if err != nil {
		return false, err
	}

	exists, err = GetRedisCli().DoHExists(rKey, rField)
	return exists, err
}

// add data to redis
func (mb *MgrBase) AddToRedis(p ...any) bool {
	if len(p) == 0 {
		logs.Error("cache:%v, add empty", mb.baseRedisKey)
		return false
	}

	redisCli := GetRedisCli()
	if redisCli == nil {
		logs.Error("Add: redisCli idx %v not exists", 0)
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := mb.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		if err = redisCli.DoHSet(rKey, rField, p[0]); err != nil {
			return false
		}
	} else {
		mData, err := mb.sliceToRedisMap(p)
		if err != nil {
			return false
		}
		for k, v := range mData {
			if err := redisCli.DoHMSet(k, v); err != nil {
				return false
			}
		}
	}

	return true
}

// get single field data with db load check
func (mb *MgrBase) GetWithCheckDbLoad(kvs []any, fvs []any, p any) (err error) {
	// cache lookup
	exists, err := mb.Get(kvs, fvs, p)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	dbQueryParams := make(map[string]any)
	for i, v := range kvs {
		if v != nil {
			dbQueryParams[mb.ksName[i]] = v
		}
	}
	for i, v := range fvs {
		if v != nil {
			dbQueryParams[mb.fsName[i]] = v
		}
	}
	if err = GetDbCli().SelectSingle(p, dbQueryParams); err != nil {
		return err
	}

	rKey, err := mb.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return err
	}
	rField, err := mb.GetRedisFieldWithVal(fvs...)
	if err != nil {
		return err
	}
	if err := GetRedisCli().DoHSet(rKey, rField, p); err != nil {
		return err
	}

	return nil
}
