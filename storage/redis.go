package storage

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	"github.com/yinyihanbing/gutils/logs"
)

type RedisCli struct {
	config *RedisConfig
	pool   *redis.Pool
}

type RedisConfig struct {
	StrAddr     string        // 连接串
	StrPwd      string        // 密码
	MaxIdle     int           // 最大的空闲连接数，表示即使没有redis连接时依然可以保持N个空闲的连接，而不被清除，随时处于待命状态
	MaxActive   int           // 最大的激活连接数，表示同时最多有N个连接 ，为0事表示没有限制
	IdleTimeout time.Duration // 最大的空闲连接等待时间，超过此时间后，空闲连接将被关闭(秒)
	Wait        bool          // 当链接数达到最大后是否阻塞，如果不的话，达到最大后返回错误
	DB          int           // 选择的数据库编号，默认0号数据库
}

// 获取切片指针的reflect.Kind和reflect.Value
func (this *RedisCli) getPrtSliceKV(slicePrt interface{}) (v reflect.Value, err error) {
	if slicePrt == nil {
		return
	}
	sType := reflect.TypeOf(slicePrt)
	v = reflect.ValueOf(slicePrt)
	if sType.Kind() != reflect.Ptr || sType.Elem().Kind() != reflect.Slice {
		err = errors.New(fmt.Sprintf("Redis HSCAN: pFieldSlice must be slice pointer "))
		return
	}
	v = v.Elem()

	return v, nil
}

// 新建一个Redis连接池
func newRedisClipool(cfg *RedisConfig) (*RedisCli, error) {
	clipool := &redis.Pool{
		MaxIdle:     cfg.MaxIdle,
		MaxActive:   cfg.MaxActive,
		IdleTimeout: cfg.IdleTimeout,
		Wait:        cfg.Wait,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.StrAddr, redis.DialDatabase(cfg.DB), redis.DialPassword(cfg.StrPwd))
			if err != nil {
				return nil, err
			}
			if cfg.StrPwd != "" {
				if _, err := c.Do("AUTH", cfg.StrPwd); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
	redisConn := &RedisCli{
		pool:   clipool,
		config: cfg,
	}

	return redisConn, nil
}

// 关闭Redis连接
func (this *RedisCli) Destroy() {
	if this.pool != nil {
		err := this.pool.Close()
		if err != nil {
			logs.Error("RedisClipool Close error: %v", err)
			return
		}
	}
}

// 执行Redis命令
func (this *RedisCli) Do(commandName string, args ...interface{}) (interface{}, error) {
	conn := this.pool.Get()
	defer conn.Close()

	reply, err := conn.Do(commandName, args...)
	if err != nil {
		logs.Error("Redis Do error! command=%v, err=%v", commandName, err)
		return nil, err
	}
	return reply, nil
}

// SET, key:键 v:值
func (this *RedisCli) DoSet(key interface{}, v interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	redisV, err := TransferValToRedisVal(v)
	if err != nil {
		logs.Error("Redis DoSet error! %v", err)
		return err
	}

	_, err = conn.Do("SET", key, redisV)
	if err != nil {
		logs.Error("Redis DoSet error! %v", err)
		return err
	}
	return nil
}

// GET, key:键
func (this *RedisCli) DoGet(key interface{}) (v interface{}, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("GET", key)
	if err != nil {
		logs.Error("Redis DoGet error! %v", err)
		return nil, err
	}

	return ret, nil
}

// SET 扩展, key:键 prtProtoStruct:proto结构引用
func (this *RedisCli) DoSetProto(key interface{}, prtProtoStruct interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	bytes, err := proto.Marshal(prtProtoStruct.(proto.Message))
	if err != nil {
		logs.Error("Protobuf Marshal error:%v", err)
		return err
	}
	_, err = conn.Do("SET", key, bytes)
	if err != nil {
		logs.Error("Redis Do SET error! err=%v", err)
		return err
	}
	return nil
}

// GET, key:键 prtProtoStruct:proto结构引用
func (this *RedisCli) DoGetProto(key interface{}, prtProtoStruct interface{}) (exists bool, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("GET", key)
	if err != nil {
		logs.Error("Redis DoGetProto error! %v", err)
		return false, err
	}

	if ret == nil {
		return false, nil
	}

	err = proto.Unmarshal(ret.([]byte), prtProtoStruct.(proto.Message))
	if err != nil {
		logs.Error("Redis DoGetProto error! Proto Unmarshal error! key=%v, err=%v", key, err)
		return false, err
	}
	return true, nil
}

// HSET, key:键 field:域 v:值
func (this *RedisCli) DoHSet(key interface{}, field interface{}, v interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	redisV, err := TransferValToRedisVal(v)
	if err != nil {
		logs.Error("Redis DoHSet error! %v", err)
		return err
	}

	_, err = conn.Do("HSET", key, field, redisV)
	if err != nil {
		logs.Error("Redis DoHSet error! key=%v, field=%v, err=%v", key, field, err)
		return err
	}

	return nil
}

// HGET, key:键 field:域
func (this *RedisCli) DoHGet(key interface{}, field interface{}) (v interface{}, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	return conn.Do("HGET", key, field)
}

// HGET, key:键 field:域 prtProtoStruct:proto结构引用
func (this *RedisCli) DoHGetProto(key interface{}, field interface{}, prtProtoStruct interface{}) (exists bool, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("HGET", key, field)
	if err != nil {
		logs.Error("Redis DoHGetProto error! key=%v, field=%v, err=%v", key, field, err)
		return false, err
	}

	if ret == nil {
		return false, nil
	}

	err = proto.Unmarshal(ret.([]byte), prtProtoStruct.(proto.Message))
	if err != nil {
		logs.Error("Redis DoHGetProto error! key=%v, field=%v, err=%v", key, field, err)
		return false, err
	}

	return true, nil
}

// HMSET 设置指定多项键域值 key:键 m: <k域, 值>
func (this *RedisCli) DoHMSet(key interface{}, m map[interface{}]interface{}) (err error) {
	if len(m) == 0 {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	args := make([]interface{}, 0, len(m)*2+1)
	args = append(args, key)
	for k, v := range m {
		val, err := TransferValToRedisVal(v)
		if err != nil {
			logs.Error("Redis DoHMSet error! key=%v, err=%v", key, err)
			return err
		}
		args = append(args, k, val)
	}

	_, err = conn.Do("HMSET", args...)
	if err != nil {
		logs.Error("Redis DoHMSet error! key=%v,err=%v", key, err)
		return err
	}

	return nil
}

// HMGET 获取指定键域中的值 key:键  prtSlice:结构引用切片, fields:域
func (this *RedisCli) DoHMGet(key interface{}, prtSlice interface{}, fieldValues ...interface{}) (err error) {
	if len(fieldValues) == 0 {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	args := make([]interface{}, 0, len(fieldValues)+1)
	args = append(args, key)
	args = append(args, fieldValues...)

	values, err := redis.Values(conn.Do("HMGET", args...))
	if err != nil {
		logs.Error("Redis DoHMGet error! key=%v, err=%v", key, err)
		return
	}
	if values == nil {
		return
	}

	pt := reflect.TypeOf(prtSlice)
	rv := reflect.ValueOf(prtSlice).Elem()

	for _, v := range values {
		if v != nil {
			newItem, err := TransferRedisValToVal(v, pt)
			if err != nil {
				logs.Error("Redis DoHMGet error! key=%v, err=%v", key, err)
				return err
			}
			rv.Set(reflect.Append(rv, reflect.ValueOf(newItem)))
		}
	}

	return nil
}

// HVals 获取指定键中的值 key:键  prtSlice:结构引用切片
func (this *RedisCli) DoHVals(key interface{}, prtSlice interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HVALS", key))
	if err != nil {
		logs.Error("Redis DoHVals error! key=%v, err=%v", key, err)
		return err
	}

	pt := reflect.TypeOf(prtSlice)
	pv := reflect.ValueOf(prtSlice).Elem()

	for _, v := range values {
		if v != nil {
			newItem, err := TransferRedisValToVal(v, pt)
			if err != nil {
				logs.Error("Redis DoHVals error! key=%v, err=%v", key, err)
				return err
			}
			pv.Set(reflect.Append(pv, reflect.ValueOf(newItem)))
		}
	}

	return nil
}

// HLEN 返回键中域数量 key：键
func (this *RedisCli) DoHLen(key interface{}) (int64, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("HLEN", key))
	if err != nil {
		logs.Error("Redis DoHLen error! key=%v, err=%v", key, err)
		return 0, err
	}
	return v, nil
}

// EXISTS 判断键是否存在, key：键
func (this *RedisCli) DoExists(key interface{}) (bool, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, e := redis.Int64(conn.Do("EXISTS", key))
	if e != nil {
		logs.Error("Redis DoExists error! key=%v, err=%v", key, e)
		return false, e
	}
	return v > 0, nil
}

// HEXISTS 判断键中域是否存在, key：键, field：域
func (this *RedisCli) DoHExists(key interface{}, field interface{}) (bool, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("HEXISTS", key, field))
	if err != nil {
		logs.Error("Redis DoHExists error! key=%v, field=%v, err=%v", key, field, err)
		return false, err
	}
	return v > 0, nil
}

// INCR 获取自增, key：键
func (this *RedisCli) DoIncr(key interface{}) (int64, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, e := redis.Int64(conn.Do("INCR", key))
	if e != nil {
		logs.Error("Redis DoIncr error! key=%v, err=%v", key, e)
		return 0, e
	}
	return v, nil
}

// DEL 删除指定键, keys：一个或多个键
func (this *RedisCli) DoDel(keys ...interface{}) error {
	conn := this.pool.Get()
	defer conn.Close()

	_, e := redis.Int64(conn.Do("DEL", keys...))
	if e != nil {
		logs.Error("Redis DoDel error! key=%v, err=%v", keys, e)
		return e
	}
	return nil
}

// HDEL 删除指定键域, key：键, fields：域
func (this *RedisCli) DoHDel(key interface{}, fields ...interface{}) error {
	if len(fields) == 0 {
		return nil
	}
	conn := this.pool.Get()
	defer conn.Close()

	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	args = append(args, fields...)

	_, err := redis.Int64(conn.Do("HDEL", args...))
	if err != nil {
		logs.Error("Redis DoHDel error! key=%v, field=%v, err=%v", key, args, err)
		return err
	}
	return nil
}

// HKEYS 获取域列表 key:键  prtFieldSlice:用于存储域的切片引用
func (this *RedisCli) DoHKeys(key interface{}, prtFieldSlice interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HKEYS", key))
	if err != nil {
		logs.Error("Redis DoHKeys error! key=%v, err=%v", key, err)
		return
	}

	rt := reflect.TypeOf(prtFieldSlice)
	rv := reflect.ValueOf(prtFieldSlice).Elem()

	for _, v := range values {
		if v != nil {
			newItem, err := TransferRedisValToVal(v, rt)
			if err != nil {
				logs.Error("Redis DoHKeys error! key=%v, err=%v", key, err)
				return err
			}
			rv.Set(reflect.Append(rv, reflect.ValueOf(newItem)))
		}
	}

	return err
}

// ZADD, key:键 params=score,member ...
func (this *RedisCli) DoZAdd(key interface{}, params ...interface{}) (err error) {
	if len(params) == 0 {
		err = errors.New(fmt.Sprintf("DoZAdd: args is empty, key=%v", key))
		logs.Error("%v", err)
		return
	}
	if len(params)%2 != 0 {
		err = errors.New(fmt.Sprintf("DoZAdd: args is error, key=%v", key))
		logs.Error("%v", err)
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	args := make([]interface{}, len(params)+1)
	args[0] = key
	copy(args[1:], params)

	_, err = conn.Do("ZADD", args...)
	if err != nil {
		logs.Error("Redis DoZAdd error! key=%v, err=%v", key, err)
		return err
	}

	return nil
}

// ZREVRANGE, key:键, membersSlicePrt 切片指针, start, stop 范围(包含)
func (this *RedisCli) DoZRevRange(key interface{}, membersSlicePrt interface{}, start, stop int) (err error) {
	pt := reflect.TypeOf(membersSlicePrt)

	mValue, err := this.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, stop))
	if err != nil {
		logs.Error("Redis DoZRevRange error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
		return
	}
	if values == nil {
		return
	}

	for _, v := range values {
		member, err := TransferRedisValToVal(v, pt)
		if err != nil {
			return err
		}
		mValue.Set(reflect.Append(mValue, reflect.ValueOf(member)))
	}

	return nil
}

// ZREVRANGE, key:键, membersSlicePrt, scoreSlicePrt 切片指针,start, stop 范围(包含)
func (this *RedisCli) DoZRevRangeWithScores(key interface{}, membersSlicePrt interface{}, scoreSlicePrt interface{}, start, stop int) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)
	spt := reflect.TypeOf(scoreSlicePrt)

	sValue, err := this.getPrtSliceKV(scoreSlicePrt)
	if err != nil {
		return
	}
	mValue, err := this.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	if err != nil {
		logs.Error("Redis DoZRangeWithScores error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
		return
	}
	if values == nil {
		return
	}

	l := len(values)
	for i := 0; i < l; i += 2 {
		if i+1 < l && values[i] != nil && values[i+1] != nil {
			idx, err := TransferRedisValToVal(values[i+1], spt)
			if err != nil {
				return err
			}
			sValue.Set(reflect.Append(sValue, reflect.ValueOf(idx)))

			member, err := TransferRedisValToVal(values[i], mpt)
			if err != nil {
				return err
			}
			mValue.Set(reflect.Append(mValue, reflect.ValueOf(member)))
		}
	}

	return nil
}

// ZRANGE, key:键, scoreSlicePrt, membersSlicePrt 切片指针,start, stop 范围(包含)
func (this *RedisCli) DoZRangeWithScores(key interface{}, scoreSlicePrt interface{}, membersSlicePrt interface{}, start, stop int) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)
	spt := reflect.TypeOf(scoreSlicePrt)

	sValue, err := this.getPrtSliceKV(scoreSlicePrt)
	if err != nil {
		return
	}
	mValue, err := this.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	if err != nil {
		logs.Error("Redis DoZRangeWithScores error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
		return
	}
	if values == nil {
		return
	}

	l := len(values)
	for i := 0; i < l; i += 2 {
		if i+1 < l && values[i] != nil && values[i+1] != nil {
			idx, err := TransferRedisValToVal(values[i+1], spt)
			if err != nil {
				return err
			}
			sValue.Set(reflect.Append(sValue, reflect.ValueOf(idx)))

			member, err := TransferRedisValToVal(values[i], mpt)
			if err != nil {
				return err
			}
			mValue.Set(reflect.Append(mValue, reflect.ValueOf(member)))
		}
	}

	return nil
}

// ZRANGE, key:键, membersSlicePrt 切片指针,start, stop 范围(包含)
func (this *RedisCli) DoZRange(key interface{}, membersSlicePrt interface{}, start, stop int) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)

	mType := reflect.TypeOf(membersSlicePrt)
	mValue := reflect.ValueOf(membersSlicePrt)
	if mType.Kind() != reflect.Ptr || mType.Elem().Kind() != reflect.Slice {
		err = errors.New(fmt.Sprintf("Redis DoZRange: membersSlicePrt must be slice pointer "))
	}
	mValue = mValue.Elem()

	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGE", key, start, stop))
	if err != nil {
		logs.Error("Redis DoZRange error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
		return
	}
	if values == nil {
		return
	}

	for _, v := range values {
		member, err := TransferRedisValToVal(v, mpt)
		if err != nil {
			return err
		}
		mValue.Set(reflect.Append(mValue, reflect.ValueOf(member)))
	}

	return nil
}

// ZRANGEBYSCORE, key:键, socresSlicePrt, membersSlicePrt 切片指针, minScore, maxScore 整数值或双精度浮点数(包含)
func (this *RedisCli) DoZRangeByScoreWithScores(key interface{}, scoresSlicePrt interface{}, membersSlicePrt interface{}, minScore, maxScore interface{}) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)
	spt := reflect.TypeOf(scoresSlicePrt)

	sValue, err := this.getPrtSliceKV(scoresSlicePrt)
	if err != nil {
		return
	}
	mValue, err := this.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES"))
	if err != nil {
		logs.Error("Redis DoZRangeByScoreWithScores error! key=%v, minScore=%v, maxScore=%v, err=%v", key, minScore, maxScore, err)
		return err
	}
	if values == nil {
		return
	}

	l := len(values)
	for i := 0; i < l; i += 2 {
		if i+1 < l && values[i] != nil && values[i+1] != nil {
			score, err := TransferRedisValToVal(values[i+1], spt)
			if err != nil {
				return err
			}
			sValue.Set(reflect.Append(sValue, reflect.ValueOf(score)))

			member, err := TransferRedisValToVal(values[i], mpt)
			if err != nil {
				return err
			}
			mValue.Set(reflect.Append(mValue, reflect.ValueOf(member)))
		}
	}

	return nil
}

// ZRANGEBYSCORE, key:键, membersSlicePrt 切片指针, minScore, maxScore 整数值或双精度浮点数(包含)
func (this *RedisCli) DoZRangeByScore(key interface{}, membersSlicePrt interface{}, minScore, maxScore interface{}) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)

	mValue, err := this.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, minScore, maxScore))
	if err != nil {
		logs.Error("Redis DoZRangeByScore error! key=%v, minScore=%v, maxScore=%v, err=%v", key, minScore, maxScore, err)
		return err
	}
	if values == nil {
		return
	}

	for _, v := range values {
		member, err := TransferRedisValToVal(v, mpt)
		if err != nil {
			return err
		}
		mValue.Set(reflect.Append(mValue, reflect.ValueOf(member)))
	}

	return nil
}

// ZREM 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。, key：键, member: 一个或多个成员
func (this *RedisCli) DoZRem(key interface{}, member ...interface{}) error {
	conn := this.pool.Get()
	defer conn.Close()

	args := make([]interface{}, 0, len(member)+1)
	args = append(args, key)
	args = append(args, member...)

	_, e := redis.Int64(conn.Do("ZREM", args...))
	if e != nil {
		logs.Error("Redis DoZRem error! key=%v, err=%v", key, e)
		return e
	}
	return nil
}

// ZREVRANK key:键, rank排名 第1名为0, 无排名为-1 (降序)
func (this *RedisCli) DoZRevRank(key interface{}, member interface{}) (rank int, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("ZREVRANK", key, member)
	if err != nil {
		logs.Error("Redis DoZRevRank error! err=%v", err)
		return 0, err
	}

	if ret == nil {
		return -1, nil
	}

	rank, err = redis.Int(ret, err)

	return rank, err
}

// ZSCORE key:键, 返回有序集 key 中，成员 member 的 score 值。
func (this *RedisCli) DoZScore(key interface{}, member interface{}) (score int64, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("ZSCORE", key, member)
	if err != nil {
		logs.Error("Redis DoZRevRank error! err=%v", err)
		return 0, err
	}

	if ret == nil {
		return 0, nil
	}

	strV, err := redis.String(ret, err)
	if err != nil {
		return 0, err
	}

	score, err = strconv.ParseInt(strV, 10, 64)

	return score, err
}

// ZCARD 返回有序集 key 的基数, key：键
func (this *RedisCli) DoZCARD(key interface{}) (int64, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("ZCARD", key))
	if err != nil {
		logs.Error("Redis Do ZCARD error! key=%v, err=%v", key, err)
		return 0, err
	}
	return v, nil
}

// EXPIRE 为key设置过期时间, key:键 timestamp:过期时间(秒)
func (this *RedisCli) DoExpire(key interface{}, t int64) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	_, err = conn.Do("EXPIRE", key, t)
	if err != nil {
		logs.Error("Redis Do EXPIRE error! key=%v, t=%v, err=%v", key, t, err)
		return err
	}

	return nil
}

// 使用迭代方式读取数据, key:键 key=键, match=匹配条件,* 默认所有(例: *、*A、*A*、A*), pF:域指针, pV:值指针, iterFunc:迭代执行函数
func (this *RedisCli) DoHScan(key interface{}, match string, pF, pV interface{}, iterFunc func(f interface{}, v interface{}, err error) bool) {
	conn := this.pool.Get()
	defer conn.Close()

	var cursor int64
	ft := reflect.TypeOf(pF)
	vt := reflect.TypeOf(pV)

	for {
		reply, err := redis.Values(conn.Do("HSCAN", key, cursor, "match", match))
		if err != nil {
			logs.Error("Redis DoHScan error! key=%v, match=%v, err=%v", key, match, err)
			iterFunc(nil, nil, err)
			break
		}
		if len(reply) != 2 {
			err := errors.New(fmt.Sprintf("HSCAN reply slice len error"))
			logs.Error("Redis DoHScan error! key=%v, match=%v, err=%v", key, match, err)
			iterFunc(nil, nil, err)
			break
		}
		cursor, err = redis.Int64(reply[0], err)
		if err != nil {
			logs.Error("Redis DoHScan error! key=%v, match=%v, err=%v", key, match, err)
			iterFunc(nil, nil, err)
			break
		}
		values := reply[1].([]interface{})
		l := len(values)
		for i := 0; i < l; i += 2 {
			if i+1 < l && values[i] != nil && values[i+1] != nil {
				field, err := TransferRedisValToVal(values[i], ft)
				if err != nil {
					logs.Error("Redis DoHScan error! key=%v, match=%v, err=%v", key, match, err)
					iterFunc(nil, nil, err)
					cursor = 0
					break
				}
				value, err := TransferRedisValToVal(values[i+1], vt)
				if err != nil {
					logs.Error("Redis DoHScan error! key=%v, match=%v, err=%v", key, match, err)
					iterFunc(nil, nil, err)
					cursor = 0
					break
				}
				if !iterFunc(field, value, err) {
					cursor = 0
					break
				}
			}
		}

		if cursor == 0 {
			break
		}
	}
}

// LPUSH, key:键, p=protobuf结构引用
func (this *RedisCli) DoLPushExt(key interface{}, p interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	bytes, err := proto.Marshal(p.(proto.Message))
	if err != nil {
		logs.Error("Redis DoLPush Protobuf Marshal error! key=%v, err=%v", key, err)
		return err
	}

	_, err = conn.Do("LPUSH", key, bytes)
	if err != nil {
		logs.Error("Redis DoLPush error! key=%v, err=%v", key, err)
		return err
	}

	return nil
}

// LRANGE, key:键, slicePrt=protobuf结构引用切片, start, stop 范围(包含)
func (this *RedisCli) DoLRangeExt(key interface{}, slicePrt interface{}, start, stop int) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("LRANGE", key, start, stop))
	if err != nil {
		logs.Error("Redis DoLRangeExt error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
		return
	}
	if values == nil {
		return
	}

	pt := reflect.TypeOf(slicePrt)
	results := reflect.ValueOf(slicePrt)
	if results.Kind() == reflect.Ptr {
		results = results.Elem()
	}

	for _, v := range values {
		if v != nil {
			newItem, err := TransferRedisValToVal(v, pt)
			if err != nil {
				logs.Error("Redis DoLRangeExt Protobuf Unmarshal error! key=%v, err=%v", key, err)
				return err
			}
			results.Set(reflect.Append(results, reflect.ValueOf(newItem)))
		}
	}

	return nil
}

// LLEN, key：键
func (this *RedisCli) DoLLen(key interface{}) (int64, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("LLEN", key))
	if err != nil {
		logs.Error("Redis DoLLen error! key=%v, err=%v", key, err)
		return 0, err
	}
	return v, nil
}

// LTRIM, key：键, start, stop 范围(包含)
func (this *RedisCli) DoLTrim(key interface{}, start, stop int) (bool, error) {
	conn := this.pool.Get()
	defer conn.Close()

	v, err := redis.Bool(conn.Do("LTRIM", key, start, stop))
	if err != nil {
		logs.Error("Redis DoLTrim error! key=%v, err=%v", key, err)
		return false, err
	}
	return v, nil
}

// LPOP, key:键, v=值
func (this *RedisCli) DoLPop(key interface{}) (v interface{}, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	return conn.Do("LPOP", key)
}

// RPUSH, key:键, v=值
func (this *RedisCli) DoRPush(key interface{}, v interface{}) (err error) {
	conn := this.pool.Get()
	defer conn.Close()

	_, err = conn.Do("RPUSH", key, v)
	if err != nil {
		logs.Error("Redis DoRPush error! key=%v, err=%v", key, err)
		return err
	}

	return nil
}

// ZRANK key:键, rank排名 第1名为0, 无排名为-1 (升序)
func (this *RedisCli) DoZRank(key interface{}, member interface{}) (rank int, err error) {
	conn := this.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("ZRANK", key, member)
	if err != nil {
		logs.Error("Redis DoZRank error! err=%v", err)
		return 0, err
	}

	if ret == nil {
		return -1, nil
	}

	rank, err = redis.Int(ret, err)

	return rank, err
}
