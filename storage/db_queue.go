package storage

import (
	"fmt"
	"sync"
	"time"
	"runtime"

	"github.com/garyburd/redigo/redis"
	"github.com/yinyihanbing/gutils"
	"github.com/yinyihanbing/gutils/logs"
)

type DbQueueType int

const (
	DbQueueTypeNone   DbQueueType = 0 // 无队列
	DbQueueTypeMemory DbQueueType = 1 // 内存队列
	DbQueueTypeRedis  DbQueueType = 2 // Redis队列
)

// 数据库写入队列
type DbQueue struct {
	QueueType        DbQueueType // 队列类型
	QueueLimitCount  int         // sql队列上限数, 超过上限必需等待
	QueueRedisCliIdx int         // Redis连接池编号
	QueueDbCliIdx    int         // Db连接池编号
	RedisQueueKey    string      // Redis队列Key
	chanSql          chan string
	wg               sync.WaitGroup
	closeFlag        bool
	lock             sync.Mutex

	timerHelper *gutils.TimerHelper
	Dcr         *DbQueueDcr
}

// 采集队列信息
type DbQueueDcr struct {
	PutCount  uint64 // 放入数量
	ExecCount uint64 // 执行数量
}

// 实例化队列, queueType=队列类型, queueRedisCliIdx=Reids连接池编号, dbCliIdx=数据库连接池编号, queueLimitCount=队列条数上限
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

// 写入队列
func (this *DbQueue) PutToQueue(strSql string) {
	if this.closeFlag {
		logs.Error("cannot put in queue! db queue stopping, db idx=%v, sql=%v", this.QueueDbCliIdx, strSql)
		return
	}

	switch this.QueueType {
	case DbQueueTypeMemory:
		this.chanSql <- strSql
		// 统计放入队列数量
		this.Dcr.PutCount += 1
		logs.Debug("put sql to member queue: %v", strSql)
	case DbQueueTypeRedis:
		GetRedisCliExt(this.QueueRedisCliIdx).DoRPush(this.RedisQueueKey, strSql)
		// 统计放入队列数量
		this.Dcr.PutCount += 1
		logs.Debug("put sql to redis queue: %v", strSql)
	}
}

// 启动队列
func (this *DbQueue) StartQueueTask() {
	flagShowQueueLog := true

	switch this.QueueType {
	case DbQueueTypeMemory:
		go this.startMemoryQueueTask()
	case DbQueueTypeRedis:
		go this.startRedisQueueTask()
	default:
		flagShowQueueLog = false
	}

	if flagShowQueueLog {
		// 定时输出队列信息
		this.timerHelper.CronFuncExt("0 */10 * * * *", this.OutStatusTask)
	}
}

// 内存方式队列任务
func (this *DbQueue) startMemoryQueueTask() {
	defer this.PanicError()
	this.wg.Add(1)
	defer this.wg.Done()

	for {
		select {
		case strSql := <-this.chanSql:
			if strSql == "" && len(this.chanSql) == 0 {
				logs.Info("close memory queue successfully, dbCliIdx: [%v] ", this.QueueDbCliIdx)
				return
			}

			// 写入到数据库
			_, err := GetDbCliExt(this.QueueDbCliIdx).Exec(strSql)
			if err != nil {
				logs.Error("%v", err)
			}
			// 统计执行数量
			this.Dcr.ExecCount += 1
		}
	}
}

// Redis缓存方式队列任务
func (this *DbQueue) startRedisQueueTask() {
	defer this.PanicError()
	this.wg.Add(1)
	defer this.wg.Done()

	for {
		// 获取队列数据
		ret, err := GetRedisCliExt(this.QueueRedisCliIdx).DoLPop(this.RedisQueueKey)

		// 空队列处理
		if (ret == nil && err == nil) || err == redis.ErrNil {
			if this.closeFlag {
				logs.Info("close redis queue successfully, dbCliIdx: [%v] ", this.QueueDbCliIdx)
				break
			} else {
				time.Sleep(3 * time.Second)
				continue
			}
		}

		// 异常处理
		if err != nil {
			logs.Error("Redis Do lpop error! err=%v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		strSql, err := redis.String(ret, err)
		if err != nil {
			logs.Error("Redis Do lpop error! err=%v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		dbCli := GetDbCliExt(this.QueueDbCliIdx)
		_, err = dbCli.Exec(strSql)
		if err != nil {
			logs.Error("%v", err)
		}

		// 统计执行数量
		this.Dcr.ExecCount += 1
	}
}

// 关闭队列
func (this *DbQueue) Destroy() {
	this.lock.Lock()
	defer this.lock.Unlock()

	if this.closeFlag == false {
		this.closeFlag = true

		switch this.QueueType {
		case DbQueueTypeMemory:
			this.chanSql <- ""
			logs.Info("waiting memory queue close... dbCliIdx: [%v], count=%v", this.QueueDbCliIdx, this.GetQueueCount())
			this.wg.Wait()
		case DbQueueTypeRedis:
			logs.Info("waiting redis queue close... dbCliIdx: [%v], count=%v", this.QueueDbCliIdx, this.GetQueueCount())
			this.wg.Wait()
		}

		// 停止定时器
		this.timerHelper.Stop()
	}
}

// 获取队列数量
func (this *DbQueue) GetQueueCount() int64 {
	switch this.QueueType {
	case DbQueueTypeMemory:
		return int64(len(this.chanSql))
	case DbQueueTypeRedis:
		count, err := GetRedisCliExt(this.QueueRedisCliIdx).DoLLen(this.RedisQueueKey)
		if err != nil {
			logs.Error("get redis queue count err: %v", err)
			return 0
		}
		return count
	}
	return 0
}

// 输出状态任务
func (this *DbQueue) OutStatusTask() {
	logs.Info("[db%v] queue count = %v", this.QueueDbCliIdx, this.GetQueueCount())
	logs.Info("[db%v] put count = %v", this.QueueDbCliIdx, this.Dcr.PutCount)
	logs.Info("[db%v] exec count = %v", this.QueueDbCliIdx, this.Dcr.ExecCount)
}

// 崩溃错误处理
func (this *DbQueue) PanicError() {
	if r := recover(); r != nil {
		buf := make([]byte, 4096)
		l := runtime.Stack(buf, false)
		err := fmt.Errorf("%v: %s", r, buf[:l])
		logs.Fatal(err)
	}
}
