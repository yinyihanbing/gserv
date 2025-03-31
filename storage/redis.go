package storage

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yinyihanbing/gutils/logs"
	"google.golang.org/protobuf/proto"
)

type RedisCli struct {
	config *RedisConfig
	pool   *redis.Pool
}

type RedisConfig struct {
	StrAddr     string        // redis connection string
	StrPwd      string        // redis password
	MaxIdle     int           // max idle connections
	MaxActive   int           // max active connections, 0 means no limit
	IdleTimeout time.Duration // max idle timeout in seconds
	Wait        bool          // block when max connections are reached
	DB          int           // redis database index, default is 0
}

// getPrtSliceKV retrieves the reflect.Kind and reflect.Value of a slice pointer.
func (rc *RedisCli) getPrtSliceKV(slicePrt any) (v reflect.Value, err error) {
	if slicePrt == nil {
		return
	}
	sType := reflect.TypeOf(slicePrt)
	v = reflect.ValueOf(slicePrt)
	if sType.Kind() != reflect.Ptr || sType.Elem().Kind() != reflect.Slice {
		err = errors.New("redis hscan: pFieldSlice must be a slice pointer")
		return
	}
	v = v.Elem()
	return v, nil
}

// newRedisClipool creates a new redis connection pool.
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

// Destroy closes the redis connection pool.
func (rc *RedisCli) Destroy() {
	if rc.pool != nil {
		err := rc.pool.Close()
		if err != nil {
			logs.Error("redis pool close error: %v", err)
			return
		}
	}
}

// Do executes a redis command.
func (rc *RedisCli) Do(commandName string, args ...any) (any, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	reply, err := conn.Do(commandName, args...)
	if err != nil {
		logs.Error("redis do error! command=%v, err=%v", commandName, err)
		return nil, err
	}
	return reply, nil
}

// DoSet sets a key-value pair in redis.
func (rc *RedisCli) DoSet(key any, v any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	redisV, err := TransferValToRedisVal(v)
	if err != nil {
		logs.Error("redis doset error! %v", err)
		return err
	}

	_, err = conn.Do("SET", key, redisV)
	if err != nil {
		logs.Error("redis doset error! %v", err)
		return err
	}
	return nil
}

// DoGet retrieves the value of a key from redis.
func (rc *RedisCli) DoGet(key any) (v any, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("GET", key)
	if err != nil {
		logs.Error("redis doget error! %v", err)
		return nil, err
	}
	return ret, nil
}

// DoSetProto sets a key-value pair in redis with a protobuf structure.
func (rc *RedisCli) DoSetProto(key any, prtProtoStruct any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	bytes, err := proto.Marshal(prtProtoStruct.(proto.Message))
	if err != nil {
		logs.Error("protobuf marshal error:%v", err)
		return err
	}
	_, err = conn.Do("SET", key, bytes)
	if err != nil {
		logs.Error("redis do set error! err=%v", err)
		return err
	}
	return nil
}

// DoGetProto retrieves the value of a key from redis and unmarshals it into a protobuf structure.
func (rc *RedisCli) DoGetProto(key any, prtProtoStruct any) (exists bool, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("GET", key)
	if err != nil {
		logs.Error("redis dogetproto error! %v", err)
		return false, err
	}

	if ret == nil {
		return false, nil
	}

	err = proto.Unmarshal(ret.([]byte), prtProtoStruct.(proto.Message))
	if err != nil {
		logs.Error("redis dogetproto error! proto unmarshal error! key=%v, err=%v", key, err)
		return false, err
	}
	return true, nil
}

// DoHSet sets a field-value pair in a hash.
func (rc *RedisCli) DoHSet(key any, field any, v any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	redisV, err := TransferValToRedisVal(v)
	if err != nil {
		logs.Error("redis dohset error! %v", err)
		return err
	}

	_, err = conn.Do("HSET", key, field, redisV)
	if err != nil {
		logs.Error("redis dohset error! key=%v, field=%v, err=%v", key, field, err)
		return err
	}

	return nil
}

// DoHGet retrieves the value of a field from a hash.
func (rc *RedisCli) DoHGet(key any, field any) (v any, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	return conn.Do("HGET", key, field)
}

// DoHGetProto retrieves the value of a field from a hash and unmarshals it into a protobuf structure.
func (rc *RedisCli) DoHGetProto(key any, field any, prtProtoStruct any) (exists bool, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("HGET", key, field)
	if err != nil {
		logs.Error("redis dohgetproto error! key=%v, field=%v, err=%v", key, field, err)
		return false, err
	}

	if ret == nil {
		return false, nil
	}

	err = proto.Unmarshal(ret.([]byte), prtProtoStruct.(proto.Message))
	if err != nil {
		logs.Error("redis dohgetproto error! key=%v, field=%v, err=%v", key, field, err)
		return false, err
	}

	return true, nil
}

// DoHMSet sets multiple field-value pairs in a hash.
func (rc *RedisCli) DoHMSet(key any, m map[any]any) (err error) {
	if len(m) == 0 {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	args := make([]any, 0, len(m)*2+1)
	args = append(args, key)
	for k, v := range m {
		val, err := TransferValToRedisVal(v)
		if err != nil {
			logs.Error("redis dohmset error! key=%v, err=%v", key, err)
			return err
		}
		args = append(args, k, val)
	}

	_, err = conn.Do("HMSET", args...)
	if err != nil {
		logs.Error("redis dohmset error! key=%v,err=%v", key, err)
		return err
	}

	return nil
}

// DoHMGet retrieves multiple field values from a hash.
func (rc *RedisCli) DoHMGet(key any, prtSlice any, fieldValues ...any) (err error) {
	if len(fieldValues) == 0 {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	args := make([]any, 0, len(fieldValues)+1)
	args = append(args, key)
	args = append(args, fieldValues...)

	values, err := redis.Values(conn.Do("HMGET", args...))
	if err != nil {
		logs.Error("redis dohmget error! key=%v, err=%v", key, err)
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
				logs.Error("redis dohmget error! key=%v, err=%v", key, err)
				return err
			}
			rv.Set(reflect.Append(rv, reflect.ValueOf(newItem)))
		}
	}

	return nil
}

// DoHVals retrieves all values from a hash.
func (rc *RedisCli) DoHVals(key any, prtSlice any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HVALS", key))
	if err != nil {
		logs.Error("redis dohvals error! key=%v, err=%v", key, err)
		return err
	}

	pt := reflect.TypeOf(prtSlice)
	pv := reflect.ValueOf(prtSlice).Elem()

	for _, v := range values {
		if v != nil {
			newItem, err := TransferRedisValToVal(v, pt)
			if err != nil {
				logs.Error("redis dohvals error! key=%v, err=%v", key, err)
				return err
			}
			pv.Set(reflect.Append(pv, reflect.ValueOf(newItem)))
		}
	}

	return nil
}

// DoHLen returns the number of fields in a hash.
func (rc *RedisCli) DoHLen(key any) (int64, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("HLEN", key))
	if err != nil {
		logs.Error("redis dohlen error! key=%v, err=%v", key, err)
		return 0, err
	}
	return v, nil
}

// DoExists checks if a key exists.
func (rc *RedisCli) DoExists(key any) (bool, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, e := redis.Int64(conn.Do("EXISTS", key))
	if e != nil {
		logs.Error("redis doexists error! key=%v, err=%v", key, e)
		return false, e
	}
	return v > 0, nil
}

// DoHExists checks if a field exists in a hash.
func (rc *RedisCli) DoHExists(key any, field any) (bool, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("HEXISTS", key, field))
	if err != nil {
		logs.Error("redis dohexists error! key=%v, field=%v, err=%v", key, field, err)
		return false, err
	}
	return v > 0, nil
}

// DoIncr increments the value of a key.
func (rc *RedisCli) DoIncr(key any) (int64, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, e := redis.Int64(conn.Do("INCR", key))
	if e != nil {
		logs.Error("redis doincr error! key=%v, err=%v", key, e)
		return 0, e
	}
	return v, nil
}

// DoDel deletes one or more keys.
func (rc *RedisCli) DoDel(keys ...any) error {
	conn := rc.pool.Get()
	defer conn.Close()

	_, e := redis.Int64(conn.Do("DEL", keys...))
	if e != nil {
		logs.Error("redis dodel error! key=%v, err=%v", keys, e)
		return e
	}
	return nil
}

// DoHDel deletes one or more fields from a hash.
func (rc *RedisCli) DoHDel(key any, fields ...any) error {
	if len(fields) == 0 {
		return nil
	}
	conn := rc.pool.Get()
	defer conn.Close()

	args := make([]any, 0, len(fields)+1)
	args = append(args, key)
	args = append(args, fields...)

	_, err := redis.Int64(conn.Do("HDEL", args...))
	if err != nil {
		logs.Error("redis dohdel error! key=%v, field=%v, err=%v", key, args, err)
		return err
	}
	return nil
}

// DoHKeys retrieves all field names from a hash.
func (rc *RedisCli) DoHKeys(key any, prtFieldSlice any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HKEYS", key))
	if err != nil {
		logs.Error("redis dohkeys error! key=%v, err=%v", key, err)
		return
	}

	rt := reflect.TypeOf(prtFieldSlice)
	rv := reflect.ValueOf(prtFieldSlice).Elem()

	for _, v := range values {
		if v != nil {
			newItem, err := TransferRedisValToVal(v, rt)
			if err != nil {
				logs.Error("redis dohkeys error! key=%v, err=%v", key, err)
				return err
			}
			rv.Set(reflect.Append(rv, reflect.ValueOf(newItem)))
		}
	}

	return err
}

// DoZAdd adds one or more members to a sorted set.
func (rc *RedisCli) DoZAdd(key any, params ...any) (err error) {
	if len(params) == 0 {
		err = fmt.Errorf("dozadd: args is empty, key=%v", key)
		logs.Error("%v", err)
		return
	}
	if len(params)%2 != 0 {
		err = fmt.Errorf("dozadd: args is error, key=%v", key)
		logs.Error("%v", err)
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	args := make([]any, len(params)+1)
	args[0] = key
	copy(args[1:], params)

	_, err = conn.Do("ZADD", args...)
	if err != nil {
		logs.Error("redis dozadd error! key=%v, err=%v", key, err)
		return err
	}

	return nil
}

// DoZRevRange retrieves members from a sorted set in reverse order.
func (rc *RedisCli) DoZRevRange(key any, membersSlicePrt any, start, stop int) (err error) {
	pt := reflect.TypeOf(membersSlicePrt)

	mValue, err := rc.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, stop))
	if err != nil {
		logs.Error("redis dozrevrange error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
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

// DoZRevRangeWithScores retrieves members and their scores from a sorted set in reverse order.
func (rc *RedisCli) DoZRevRangeWithScores(key any, membersSlicePrt any, scoreSlicePrt any, start, stop int) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)
	spt := reflect.TypeOf(scoreSlicePrt)

	sValue, err := rc.getPrtSliceKV(scoreSlicePrt)
	if err != nil {
		return
	}
	mValue, err := rc.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
	if err != nil {
		logs.Error("redis dozrangewithscores error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
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

// DoZRangeWithScores retrieves members and their scores from a sorted set.
func (rc *RedisCli) DoZRangeWithScores(key any, scoreSlicePrt any, membersSlicePrt any, start, stop int) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)
	spt := reflect.TypeOf(scoreSlicePrt)

	sValue, err := rc.getPrtSliceKV(scoreSlicePrt)
	if err != nil {
		return
	}
	mValue, err := rc.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	if err != nil {
		logs.Error("redis dozrangewithscores error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
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

// DoZRange retrieves members from a sorted set.
func (rc *RedisCli) DoZRange(key any, membersSlicePrt any, start, stop int) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)

	mType := reflect.TypeOf(membersSlicePrt)
	mValue := reflect.ValueOf(membersSlicePrt)
	if mType.Kind() != reflect.Ptr || mType.Elem().Kind() != reflect.Slice {
		err = errors.New("redis dozrange: membersSlicePrt must be a slice pointer")
	}
	mValue = mValue.Elem()

	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGE", key, start, stop))
	if err != nil {
		logs.Error("redis dozrange error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
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

// DoZRangeByScoreWithScores retrieves members and their scores from a sorted set within a score range.
func (rc *RedisCli) DoZRangeByScoreWithScores(key any, scoresSlicePrt any, membersSlicePrt any, minScore, maxScore any) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)
	spt := reflect.TypeOf(scoresSlicePrt)

	sValue, err := rc.getPrtSliceKV(scoresSlicePrt)
	if err != nil {
		return
	}
	mValue, err := rc.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES"))
	if err != nil {
		logs.Error("redis dozrangebyscorewithscores error! key=%v, minScore=%v, maxScore=%v, err=%v", key, minScore, maxScore, err)
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

// DoZRangeByScore retrieves members from a sorted set within a score range.
func (rc *RedisCli) DoZRangeByScore(key any, membersSlicePrt any, minScore, maxScore any) (err error) {
	mpt := reflect.TypeOf(membersSlicePrt)

	mValue, err := rc.getPrtSliceKV(membersSlicePrt)
	if err != nil {
		return
	}

	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, minScore, maxScore))
	if err != nil {
		logs.Error("redis dozrangebyscore error! key=%v, minScore=%v, maxScore=%v, err=%v", key, minScore, maxScore, err)
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

// DoZRem removes one or more members from a sorted set.
func (rc *RedisCli) DoZRem(key any, member ...any) error {
	conn := rc.pool.Get()
	defer conn.Close()

	args := make([]any, 0, len(member)+1)
	args = append(args, key)
	args = append(args, member...)

	_, e := redis.Int64(conn.Do("ZREM", args...))
	if e != nil {
		logs.Error("redis dozrem error! key=%v, err=%v", key, e)
		return e
	}
	return nil
}

// DoZRevRank returns the rank of a member in a sorted set in reverse order.
func (rc *RedisCli) DoZRevRank(key any, member any) (rank int, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("ZREVRANK", key, member)
	if err != nil {
		logs.Error("redis dozrevrank error! err=%v", err)
		return 0, err
	}

	if ret == nil {
		return -1, nil
	}

	rank, err = redis.Int(ret, err)

	return rank, err
}

// DoZScore returns the score of a member in a sorted set.
func (rc *RedisCli) DoZScore(key any, member any) (score int64, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("ZSCORE", key, member)
	if err != nil {
		logs.Error("redis dozrevrank error! err=%v", err)
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

// DoZCARD returns the cardinality of a sorted set.
func (rc *RedisCli) DoZCARD(key any) (int64, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("ZCARD", key))
	if err != nil {
		logs.Error("redis do zcard error! key=%v, err=%v", key, err)
		return 0, err
	}
	return v, nil
}

// DoExpire sets an expiration time for a key.
func (rc *RedisCli) DoExpire(key any, t int64) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	_, err = conn.Do("EXPIRE", key, t)
	if err != nil {
		logs.Error("redis do expire error! key=%v, t=%v, err=%v", key, t, err)
		return err
	}
	return nil
}

// DoHScan iterates over fields and values in a hash.
func (rc *RedisCli) DoHScan(key any, match string, pF, pV any, iterFunc func(f any, v any, err error) bool) {
	conn := rc.pool.Get()
	defer conn.Close()

	var cursor int64
	ft := reflect.TypeOf(pF)
	vt := reflect.TypeOf(pV)

	for {
		reply, err := redis.Values(conn.Do("HSCAN", key, cursor, "match", match))
		if err != nil {
			logs.Error("redis dohscan error! key=%v, match=%v, err=%v", key, match, err)
			iterFunc(nil, nil, err)
			break
		}
		if len(reply) != 2 {
			err := errors.New("hscan reply slice len error")
			logs.Error("redis dohscan error! key=%v, match=%v, err=%v", key, match, err)
			iterFunc(nil, nil, err)
			break
		}
		cursor, err = redis.Int64(reply[0], err)
		if err != nil {
			logs.Error("redis dohscan error! key=%v, match=%v, err=%v", key, match, err)
			iterFunc(nil, nil, err)
			break
		}
		values := reply[1].([]any)
		l := len(values)
		for i := 0; i < l; i += 2 {
			if i+1 < l && values[i] != nil && values[i+1] != nil {
				field, err := TransferRedisValToVal(values[i], ft)
				if err != nil {
					logs.Error("redis dohscan error! key=%v, match=%v, err=%v", key, match, err)
					iterFunc(nil, nil, err)
					cursor = 0
					break
				}
				value, err := TransferRedisValToVal(values[i+1], vt)
				if err != nil {
					logs.Error("redis dohscan error! key=%v, match=%v, err=%v", key, match, err)
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

// DoLPushExt pushes a protobuf structure to a list.
func (rc *RedisCli) DoLPushExt(key any, p any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	bytes, err := proto.Marshal(p.(proto.Message))
	if err != nil {
		logs.Error("redis dolpush protobuf marshal error! key=%v, err=%v", key, err)
		return err
	}

	_, err = conn.Do("LPUSH", key, bytes)
	if err != nil {
		logs.Error("redis dolpush error! key=%v, err=%v", key, err)
		return err
	}

	return nil
}

// DoLRangeExt retrieves elements from a list within a range.
func (rc *RedisCli) DoLRangeExt(key any, slicePrt any, start, stop int) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("LRANGE", key, start, stop))
	if err != nil {
		logs.Error("redis dolrangeext error! key=%v, start=%v, stop=%v, err=%v", key, start, stop, err)
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
				logs.Error("redis dolrangeext protobuf unmarshal error! key=%v, err=%v", key, err)
				return err
			}
			results.Set(reflect.Append(results, reflect.ValueOf(newItem)))
		}
	}

	return nil
}

// DoLLen returns the length of a list.
func (rc *RedisCli) DoLLen(key any) (int64, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, err := redis.Int64(conn.Do("LLEN", key))
	if err != nil {
		logs.Error("redis dollen error! key=%v, err=%v", key, err)
		return 0, err
	}
	return v, nil
}

// DoLTrim trims a list to the specified range.
func (rc *RedisCli) DoLTrim(key any, start, stop int) (bool, error) {
	conn := rc.pool.Get()
	defer conn.Close()

	v, err := redis.Bool(conn.Do("LTRIM", key, start, stop))
	if err != nil {
		logs.Error("redis doltrim error! key=%v, err=%v", key, err)
		return false, err
	}
	return v, nil
}

// DoLPop removes and returns the first element of a list.
func (rc *RedisCli) DoLPop(key any) (v any, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	return conn.Do("LPOP", key)
}

// DoRPush pushes an element to the end of a list.
func (rc *RedisCli) DoRPush(key any, v any) (err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	_, err = conn.Do("RPUSH", key, v)
	if err != nil {
		logs.Error("redis dorpush error! key=%v, err=%v", key, err)
		return err
	}

	return nil
}

// DoZRank returns the rank of a member in a sorted set.
func (rc *RedisCli) DoZRank(key any, member any) (rank int, err error) {
	conn := rc.pool.Get()
	defer conn.Close()

	ret, err := conn.Do("ZRANK", key, member)
	if err != nil {
		logs.Error("redis dozrank error! err=%v", err)
		return 0, err
	}

	if ret == nil {
		return -1, nil
	}

	rank, err = redis.Int(ret, err)

	return rank, err
}
