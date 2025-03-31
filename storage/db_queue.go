package storage

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yinyihanbing/gutils"
	"github.com/yinyihanbing/gutils/logs"
)

// DbQueueType defines the type of database queue
type DbQueueType int

const (
	DbQueueTypeNone   DbQueueType = 0 // no queue
	DbQueueTypeMemory DbQueueType = 1 // in-memory queue
	DbQueueTypeRedis  DbQueueType = 2 // redis queue
)

// DbQueue represents a database write queue
type DbQueue struct {
	QueueType        DbQueueType // queue type
	QueueLimitCount  int         // max number of sql in queue, blocks if exceeded
	QueueRedisCliIdx int         // redis connection pool index
	QueueDbCliIdx    int         // db connection pool index
	RedisQueueKey    string      // redis queue key
	chanSql          chan string
	wg               sync.WaitGroup
	closeFlag        bool
	lock             sync.Mutex

	timerHelper *gutils.TimerHelper
	Dcr         *DbQueueDcr
}

// DbQueueDcr collects queue statistics
type DbQueueDcr struct {
	PutCount  uint64 // number of sql added to the queue
	ExecCount uint64 // number of sql executed
}

// NewDbQueue initializes a new database queue
func NewDbQueue(queueType DbQueueType, redisCliIdx int, dbCliIdx int, queueLimitCount int) *DbQueue {
	dbQueue := new(DbQueue)
	dbQueue.QueueType = queueType
	dbQueue.QueueLimitCount = queueLimitCount
	dbQueue.QueueDbCliIdx = dbCliIdx
	dbQueue.QueueRedisCliIdx = redisCliIdx
	dbQueue.Dcr = new(DbQueueDcr)
	dbQueue.timerHelper = gutils.NewTimerHelper()

	switch queueType {
	case DbQueueTypeMemory:
		dbQueue.chanSql = make(chan string, dbQueue.QueueLimitCount)
	case DbQueueTypeRedis:
		dbQueue.RedisQueueKey = fmt.Sprintf("db_queue_%v", dbCliIdx)
	}

	return dbQueue
}

// PutToQueue adds an SQL statement to the queue
func (dq *DbQueue) PutToQueue(strSql string) {
	if dq.closeFlag {
		logs.Error("cannot put in queue! db queue stopping, db idx=%v, sql=%v", dq.QueueDbCliIdx, strSql)
		return
	}

	switch dq.QueueType {
	case DbQueueTypeMemory:
		dq.chanSql <- strSql
		// increment put count
		dq.Dcr.PutCount += 1
		logs.Debug("put sql to memory queue: %v", strSql)
	case DbQueueTypeRedis:
		GetRedisCliExt(dq.QueueRedisCliIdx).DoRPush(dq.RedisQueueKey, strSql)
		// increment put count
		dq.Dcr.PutCount += 1
		logs.Debug("put sql to redis queue: %v", strSql)
	}
}

// StartQueueTask starts the queue processing task
func (dq *DbQueue) StartQueueTask() {
	flagShowQueueLog := true

	switch dq.QueueType {
	case DbQueueTypeMemory:
		go dq.startMemoryQueueTask()
	case DbQueueTypeRedis:
		go dq.startRedisQueueTask()
	default:
		flagShowQueueLog = false
	}

	if flagShowQueueLog {
		// schedule periodic queue status output
		dq.timerHelper.CronFuncExt("0 */10 * * * *", dq.OutStatusTask)
	}
}

// startMemoryQueueTask processes the in-memory queue
func (dq *DbQueue) startMemoryQueueTask() {
	defer dq.PanicError()
	dq.wg.Add(1)
	defer dq.wg.Done()

	for strSql := range dq.chanSql {
		if strSql == "" && len(dq.chanSql) == 0 {
			logs.Info("closed memory queue successfully, dbCliIdx: [%v]", dq.QueueDbCliIdx)
			return
		}

		// execute sql in database
		_, err := GetDbCliExt(dq.QueueDbCliIdx).Exec(strSql)
		if err != nil {
			logs.Error("db exec error: %v", err)
		}
		// increment exec count
		dq.Dcr.ExecCount += 1
	}
}

// startRedisQueueTask processes the redis queue
func (dq *DbQueue) startRedisQueueTask() {
	defer dq.PanicError()
	dq.wg.Add(1)
	defer dq.wg.Done()

	for {
		// fetch data from queue
		ret, err := GetRedisCliExt(dq.QueueRedisCliIdx).DoLPop(dq.RedisQueueKey)

		// handle empty queue
		if (ret == nil && err == nil) || err == redis.ErrNil {
			if dq.closeFlag {
				logs.Info("closed redis queue successfully, dbCliIdx: [%v]", dq.QueueDbCliIdx)
				break
			} else {
				time.Sleep(3 * time.Second)
				continue
			}
		}

		// handle errors
		if err != nil {
			logs.Error("redis lpop error: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		strSql, err := redis.String(ret, err)
		if err != nil {
			logs.Error("redis lpop error: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		// execute sql in database
		dbCli := GetDbCliExt(dq.QueueDbCliIdx)
		_, err = dbCli.Exec(strSql)
		if err != nil {
			logs.Error("db exec error: %v", err)
		}

		// increment exec count
		dq.Dcr.ExecCount += 1
	}
}

// Destroy stops the queue and cleans up resources
func (dq *DbQueue) Destroy() {
	dq.lock.Lock()
	defer dq.lock.Unlock()

	if !dq.closeFlag {
		dq.closeFlag = true

		switch dq.QueueType {
		case DbQueueTypeMemory:
			dq.chanSql <- ""
			logs.Info("waiting for memory queue to close... dbCliIdx: [%v], count=%v", dq.QueueDbCliIdx, dq.GetQueueCount())
			dq.wg.Wait()
		case DbQueueTypeRedis:
			logs.Info("waiting for redis queue to close... dbCliIdx: [%v], count=%v", dq.QueueDbCliIdx, dq.GetQueueCount())
			dq.wg.Wait()
		}

		// stop timer
		dq.timerHelper.Stop()
	}
}

// GetQueueCount returns the current queue size
func (dq *DbQueue) GetQueueCount() int64 {
	switch dq.QueueType {
	case DbQueueTypeMemory:
		return int64(len(dq.chanSql))
	case DbQueueTypeRedis:
		count, err := GetRedisCliExt(dq.QueueRedisCliIdx).DoLLen(dq.RedisQueueKey)
		if err != nil {
			logs.Error("get redis queue count error: %v", err)
			return 0
		}
		return count
	}
	return 0
}

// OutStatusTask outputs the queue status periodically
func (dq *DbQueue) OutStatusTask() {
	logs.Info("[db%v] queue count = %v", dq.QueueDbCliIdx, dq.GetQueueCount())
	logs.Info("[db%v] put count = %v", dq.QueueDbCliIdx, dq.Dcr.PutCount)
	logs.Info("[db%v] exec count = %v", dq.QueueDbCliIdx, dq.Dcr.ExecCount)
}

// PanicError handles panic errors and logs the stack trace
func (dq *DbQueue) PanicError() {
	if r := recover(); r != nil {
		buf := make([]byte, 4096)
		l := runtime.Stack(buf, false)
		err := fmt.Errorf("panic recovered: %v: %s", r, buf[:l])
		logs.Fatal(err)
	}
}
