package storage

import (
	"errors"
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

// 设置Redis键前缀、键值包含的结构体属性名、域值包含的结构体属性名
func (this *MgrBase) SetRedisKeyField(baseRedisKey string, ksName, fsName []string) {
	this.baseRedisKey = baseRedisKey
	this.ksName = ksName
	this.fsName = fsName
}

// 获取RedisKey
func (this *MgrBase) GetRedisKeyWithVal(kvs ...interface{}) (string, error) {
	if len(kvs) != len(this.ksName) {
		err := errors.New(fmt.Sprintf("缓存:%v, 键所包含的属性条数不匹配, 应包含属性：%v, 现属性值:%v", this.baseRedisKey, this.ksName, kvs))
		logs.Error(err)
		return "", err
	}

	rKey := this.baseRedisKey
	for _, v := range kvs {
		rKey = fmt.Sprintf("%v_%v", rKey, v)
	}
	return rKey, nil
}

// 获取Redis域
func (this *MgrBase) GetRedisFieldWithVal(fvs ...interface{}) (string, error) {
	if len(fvs) != len(this.fsName) {
		err := errors.New(fmt.Sprintf("缓存:%v, 域所包含的属性条数不匹配, 应包含属性：%v, 现属性值:%v", this.baseRedisKey, this.fsName, fvs))
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

// 获取Redis键 根据结构体对象
func (this *MgrBase) GetRedisKeyWithObj(p interface{}) (string, error) {
	if len(this.baseRedisKey) == 0 && len(this.ksName) == 0 {
		err := errors.New(fmt.Sprintf("缓存:%v, 未设置基础键、键所包含的属性名", this.baseRedisKey))
		logs.Error(err)
		return "", err
	}

	if len(this.ksName) == 0 {
		return this.baseRedisKey, nil
	}

	pv := reflect.ValueOf(p)
	if pv.Kind() == reflect.Ptr {
		pv = pv.Elem()
	}

	kvs := make([]interface{}, 0, len(this.ksName))
	for _, v := range this.ksName {
		objKeyField := pv.FieldByName(v)
		if !objKeyField.IsValid() {
			err := errors.New(fmt.Sprintf("缓存:%v, 获取Redis键, 结构体:%v, 属性读取失败:%v", this.baseRedisKey, reflect.TypeOf(p), v))
			logs.Error(err)
			return "", err
		}
		kvs = append(kvs, objKeyField)
	}

	rKey, err := this.GetRedisKeyWithVal(kvs...)
	return rKey, err
}

// 获取Redis域 根据结构体对象
func (this *MgrBase) GetRedisFieldWithObj(p interface{}) (string, error) {
	if len(this.fsName) == 0 {
		err := errors.New(fmt.Sprintf("缓存:%v, 未设置域所包含的属性名", this.baseRedisKey))
		logs.Error(err)
		return "", err
	}

	pv := reflect.ValueOf(p)
	if pv.Kind() == reflect.Ptr {
		pv = pv.Elem()
	}

	fvs := make([]interface{}, 0, len(this.fsName))
	for _, v := range this.fsName {
		objKeyField := pv.FieldByName(v)
		if !objKeyField.IsValid() {
			err := errors.New(fmt.Sprintf("缓存:%v, 获取Redis域, 结构体:%v, 属性读取失败:%v", this.baseRedisKey, reflect.TypeOf(p), v))
			logs.Error(err)
			return "", err
		}
		fvs = append(fvs, objKeyField)
	}

	rField, err := this.GetRedisFieldWithVal(fvs...)
	return rField, err
}

// 获取Redis键、域 根据结构体对象
func (this *MgrBase) GetRedisKeyFieldWithObj(p interface{}) (rKey string, rField string, err error) {
	rKey, err = this.GetRedisKeyWithObj(p)
	if err != nil {
		return "", "", err
	}

	rField, err = this.GetRedisFieldWithObj(p)
	if err != nil {
		return "", "", err
	}

	return
}

// 将切片转成存储格式的Map
func (this *MgrBase) sliceToRedisMap(pSlice interface{}) (mData map[string]map[interface{}]interface{}, err error) {
	v := reflect.ValueOf(pSlice)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	n := v.Len()

	mData = make(map[string]map[interface{}]interface{}) // key1=redisKey, key2=redisField
	for i := 0; i < n; i++ {
		item := v.Index(i).Interface()
		rv := reflect.ValueOf(item)
		if reflect.TypeOf(rv).Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		rKey, err := this.GetRedisKeyWithObj(item)
		if err != nil {
			return nil, err
		}

		rField, err := this.GetRedisFieldWithObj(item)
		if err != nil {
			return nil, err
		}

		if _, ok := mData[rKey]; !ok {
			mData[rKey] = make(map[interface{}]interface{})
		}
		mData[rKey][rField] = item
	}

	return mData, nil
}

// 加载, pSlice=数据容器引用切片, dbQueryParams=数据库查询参数
func (this *MgrBase) ReloadDbDataToRedis(pSlice interface{}, dbQueryParams map[string]interface{}) (n int, err error) {
	// 从数据库查询
	err = GetDbCli().SelectMultiple(pSlice, dbQueryParams)
	if err != nil {
		return 0, err
	}

	// 将切片转换成指定Map结构
	mData, err := this.sliceToRedisMap(pSlice)
	if err != nil {
		return 0, err
	}

	// 存储到Redis中
	for k1, v1 := range mData {
		// 写入Redis缓存
		err = GetRedisCli().DoHMSet(k1, v1)
		if err != nil {
			return 0, err
		}

		n += len(v1)
	}

	return n, nil
}

// DB和Redis 新增数据
func (this *MgrBase) Add(p ...interface{}) bool {
	if len(p) == 0 {
		logs.Error("缓存:%v, add empty", this.baseRedisKey)
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := this.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		err = Add(rKey, rField, p[0])
		if err != nil {
			logs.Error(fmt.Sprintf("DB和Redis 增加数据失败, 结构体=%v, err=%v", reflect.TypeOf(p[0]), err))
			return false
		}
	} else {
		mData, err := this.sliceToRedisMap(p)
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

// DB和Redis 删除数据
func (this *MgrBase) Delete(p ...interface{}) bool {
	if len(p) == 0 {
		logs.Error("缓存:%v, delete empty", this.baseRedisKey)
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := this.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		err = Delete(rKey, rField, p[0])
		if err != nil {
			logs.Error(fmt.Sprintf("缓存:%v, DB和Redis 删除数据失败, 结构体=%v, err=%v", this.baseRedisKey, reflect.TypeOf(p[0]), err))
			return false
		}
	} else {
		mData, err := this.sliceToRedisMap(p)
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

// DB和Redis 修改数据
func (this *MgrBase) Update(columns []string, p ...interface{}) bool {
	if len(p) == 0 {
		logs.Error(fmt.Sprintf("缓存:%v, update empty", this.baseRedisKey))
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := this.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		err = Update(rKey, rField, p[0], columns...)
		if err != nil {
			logs.Error(fmt.Sprintf("缓存:%v, DB和Redis 修改数据失败, 结构体=%v, err=%v", this.baseRedisKey, reflect.TypeOf(p[0]), err))
			return false
		}
	} else {
		mData, err := this.sliceToRedisMap(p)
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

// 从Redis 获取单个域数据
func (this *MgrBase) GetWithSingleKVs(kv interface{}, fv interface{}, p interface{}) (exists bool, err error) {
	var kvs []interface{} = nil
	if kv != nil {
		kvs = []interface{}{kv}
	}
	var fvs []interface{} = nil
	if fv != nil {
		fvs = []interface{}{fv}
	}
	return this.Get(kvs, fvs, p)
}

// 从Redis 获取单个域数据
func (this *MgrBase) Get(kvs []interface{}, fvs []interface{}, p interface{}) (exists bool, err error) {
	rKey, err := this.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return false, err
	}

	rField, err := this.GetRedisFieldWithVal(fvs...)
	if err != nil {
		return false, err
	}

	exists, err = GetRedisCli().DoHGetProto(rKey, rField, p)
	return
}

// 从Redis获取多个域数据
func (this *MgrBase) GetMultipleWithSingleKVs(kv interface{}, fv []interface{}, p interface{}) error {
	fvs := make([][]interface{}, 0, len(fv))
	for _, v := range fv {
		fvs = append(fvs, []interface{}{v})
	}
	var kvs []interface{} = nil
	if kv != nil {
		kvs = []interface{}{kv}
	}
	return this.GetMultiple(kvs, fvs, p)
}

// 从Redis获取多个域数据
func (this *MgrBase) GetMultiple(kvs []interface{}, fvs [][]interface{}, p interface{}) error {
	rKey, err := this.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return err
	}

	args := make([]interface{}, 0)
	for _, v := range fvs {
		rField, err := this.GetRedisFieldWithVal(v...)
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

// 从Redis获取全部域数据, [0,n-1]为Redis键构造值, [n-1]为接收数据的容器引用
func (this *MgrBase) GetAll(param ...interface{}) error {
	if len(param) < 1 {
		err := errors.New(fmt.Sprintf("缓存:%v, 参数数量不正确:%v", this.baseRedisKey, param))
		logs.Error(err)
		return err
	}
	if reflect.TypeOf(param[len(param)-1]).Kind() != reflect.Ptr {
		err := errors.New(fmt.Sprintf("缓存:%v, 参数类型不正确:%v", this.baseRedisKey, param))
		logs.Error(err)
		return err
	}

	rKey, err := this.GetRedisKeyWithVal(param[:len(param)-1]...)
	if err != nil {
		return err
	}

	if err := GetRedisCli().DoHVals(rKey, param[len(param)-1]); err != nil {
		return err
	}

	return nil
}

// 查询单Key域列表数量
func (this *MgrBase) GetFieldsLen(kvs ...interface{}) (int64, error) {
	rKey, err := this.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return 0, err
	}

	l, err := GetRedisCli().DoHLen(rKey)
	if err != nil {
		return 0, err
	}

	return l, nil
}

// 获取域列表, [0,n-1]为Redis键构造值, [n-1]为接收数据的容器引用
func (this *MgrBase) GetFields(param ...interface{}) error {
	if len(param) < 2 {
		err := errors.New(fmt.Sprintf("缓存:%v, 参数数量不正确:%v", this.baseRedisKey, param))
		logs.Error(err)
		return err
	}

	rKey, err := this.GetRedisKeyWithVal(param[:len(param)-1]...)
	if err != nil {
		return err
	}

	if err := GetRedisCli().DoHKeys(rKey, param[len(param)-1]); err != nil {
		return err
	}

	return nil
}

// 是否存在指定键
func (this *MgrBase) IsKeyExists(kvs ...interface{}) (exists bool, err error) {
	rKey, err := this.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return false, err
	}

	exists, err = GetRedisCli().DoExists(rKey)
	return exists, err
}

// 是否存在指定域
func (this *MgrBase) IsFieldExists(kvs []interface{}, fvs []interface{}) (exists bool, err error) {
	rKey, err := this.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return false, err
	}

	rField, err := this.GetRedisFieldWithVal(fvs...)
	if err != nil {
		return false, err
	}

	exists, err = GetRedisCli().DoHExists(rKey, rField)
	return exists, err
}

// Redis 新增数据
func (this *MgrBase) AddToRedis(p ...interface{}) bool {
	if len(p) == 0 {
		logs.Error("缓存:%v, add empty", this.baseRedisKey)
		return false
	}

	redisCli := GetRedisCli()
	if redisCli == nil {
		logs.Error("Add: redisCli idx %v not exists", 0)
		return false
	}

	if len(p) == 1 {
		rKey, rField, err := this.GetRedisKeyFieldWithObj(p[0])
		if err != nil {
			return false
		}
		if err = redisCli.DoHSet(rKey, rField, p[0]); err != nil {
			return false
		}
	} else {
		mData, err := this.sliceToRedisMap(p)
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

// 获取单个域数据
func (this *MgrBase) GetWithCheckDbLoad(kvs []interface{}, fvs []interface{}, p interface{}) (err error) {
	// 缓存查找
	exists, err := this.Get(kvs, fvs, p)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// 缓存不存在则数据库查找加载
	dbQueryParams := make(map[string]interface{})
	for i, v := range kvs {
		if v != nil {
			dbQueryParams[this.ksName[i]] = v
		}
	}
	for i, v := range fvs {
		if v != nil {
			dbQueryParams[this.fsName[i]] = v
		}
	}
	if err = GetDbCli().SelectSingle(p, dbQueryParams); err != nil {
		return err
	}

	// 将数据库查找到的数据写入缓存
	rKey, err := this.GetRedisKeyWithVal(kvs...)
	if err != nil {
		return err
	}
	rField, err := this.GetRedisFieldWithVal(fvs...)
	if err != nil {
		return err
	}
	if err := GetRedisCli().DoHSet(rKey, rField, p); err != nil {
		return err
	}

	return nil
}
