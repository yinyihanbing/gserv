package storage

import (
	"fmt"
	"reflect"
	"time"
	"errors"
	"strings"
	"strconv"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yinyihanbing/gutils"
	"github.com/yinyihanbing/gutils/logs"
)

type DbCli struct {
	config  *DbConfig
	db      *sql.DB
	sm      *SchemaManager
	dbQueue *DbQueue // 队列

	DbName string // 数据库名
}

type DbConfig struct {
	StrAddr         string        // 连接串
	ConnMaxLifetime time.Duration // 连接超时时间
	MaxOpenConns    int           // 最大连接数
	MaxIdleConns    int           // 最大空闲连接数

	QueueType        DbQueueType // 队列类型
	QueueRedisCliIdx int         // Redis连接池编号
	QueueDbCliIdx    int         // Db连接池编号
	QueueLimitCount  int         // sql队列上限数, 超过上限必需等待
}

// 打开一个数据库连接池
func newDbCli(cfg *DbConfig) (db *DbCli, err error) {
	var d *sql.DB
	d, err = sql.Open("mysql", cfg.StrAddr)
	if err != nil {
		logs.Error("mysql connection failed: %v %v", cfg.StrAddr, err)
		return nil, err
	}
	if err = d.Ping(); err != nil {
		d.Close()
		logs.Error("mysql connection failed: %v %v", cfg.StrAddr, err)
		return nil, err
	}
	if cfg.MaxIdleConns != 0 {
		d.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	if cfg.MaxOpenConns != 0 {
		d.SetMaxOpenConns(cfg.MaxOpenConns)
	}

	if cfg.ConnMaxLifetime != 0 {
		d.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	db = &DbCli{
		config: cfg,
		db:     d,
	}

	// 初始化实体管理者
	db.sm = newSchemaManager()

	// 初始化数据库队列
	db.dbQueue = NewDbQueue(cfg.QueueType, cfg.QueueRedisCliIdx, cfg.QueueDbCliIdx, cfg.QueueLimitCount)

	// 数据库名
	db.DbName = db.CurrentDatabase()

	logs.Info("mysql connection success: %v", cfg.StrAddr)

	return db, nil
}

func (this *DbCli) Destroy() {
	this.dbQueue.Destroy()

	if this.db != nil {
		this.db.Close()
	}
}

// 启动数据库队列
func (this *DbCli) StartQueue() {
	this.dbQueue.StartQueueTask()
}

// 获取连接的数据库名
func (this *DbCli) CurrentDatabase() (name string) {
	strSql, err := CreateCurrentDatabaseSql()
	if err != nil {
		logs.Error(fmt.Sprintf("create sql error: %v", err))
		return
	}
	this.db.QueryRow(strSql).Scan(&name)
	return
}

// 获取数据库中的所有表名
func (this *DbCli) GetAllTableNames() ([]string, error) {
	strSql := CreateSelectTablesName(this.CurrentDatabase())
	logs.Debug("%v", strSql)

	rows, err := this.QueryRow(strSql)
	if err != nil {
		return nil, fmt.Errorf("sql error. %v, %v", strSql, err)
	}
	defer rows.Close()

	tableNames := make([]string, 0)
	for rows.Next() {
		var col1 string
		err = rows.Scan(&col1)
		if err != nil {
			return nil, fmt.Errorf("sql error. %v, %v", strSql, err)
		}
		tableNames = append(tableNames, col1)
	}

	return tableNames, nil
}

// 查询数据库中表结构
func (this *DbCli) GetTableStruct(tableName string) ([]*Field, error) {
	strSql := CreateSelectTableStruct(tableName)
	logs.Debug("%v", strSql)

	rows, err := this.QueryRow(strSql)
	if err != nil {
		return nil, fmt.Errorf("sql error. %v, %v", strSql, err)
	}
	defer rows.Close()

	fields := make([]*Field, 0)
	for rows.Next() {
		var col1 string
		var col2 string
		var col3 string
		var col4 string
		var col5 interface{}
		var col6 interface{}
		err = rows.Scan(&col1, &col2, &col3, &col4, &col5, &col6)
		if err != nil {
			return nil, fmt.Errorf("sql error. %v, %v", strSql, err)
		}
		f := Field{}
		f.Name = col1
		f.ColumnName = col1
		if idx := strings.Index(col2, "("); idx > 0 {
			f.ColumnType = EnumColumnType(gutils.SubString(col2, 0, strings.Index(col2, "(")))
			x, err := strconv.Atoi(gutils.SubString(col2, strings.Index(col2, "(")+1, strings.Index(col2, ")")))
			if err != nil {
				return nil, err
			}
			f.ColumnLength = int16(x)
		} else {
			f.ColumnType = EnumColumnType(col2)
			f.ColumnLength = 0
		}
		f.PrimaryKey = col4 == "PRI"

		fields = append(fields, &f)
	}

	return fields, nil
}

// 当前连接的数据库是否存在指定表
func (this *DbCli) HasTable(tableName string) (bool, error) {
	strSql, err := CreateHasTableSql(this.CurrentDatabase(), tableName)
	if err != nil {
		logs.Error(fmt.Sprintf("create sql error: %v", err))
		return false, err
	}
	logs.Debug("%v", strSql)
	var count int
	err = this.db.QueryRow(strSql).Scan(&count)
	if err != nil {
		logs.Error("db exec %v err:%v", strSql, err)
		return false, err
	}
	return count > 0, nil
}

// 获取表列中的最大值
func (this *DbCli) SelectMaxValue(tableName string, columnName string) (int64, error) {
	strSql, err := CreateColumnMaxValueSql(tableName, columnName)
	if err != nil {
		logs.Error(fmt.Sprintf("create sql error: %v", err))
		return 0, err
	}
	logs.Debug("%v", strSql)
	var count int64
	err = this.db.QueryRow(strSql).Scan(&count)
	if err != nil {
		logs.Error("db exec %v err:%v", strSql, err)
		return 0, err
	}
	return count, nil
}

// 查询多行数据
func (this *DbCli) SelectRowScanBySql(strSql string, rowPrt interface{}, rowCall func(rowPrt interface{}) error) (err error) {
	rows, errQuery := this.QueryRow(strSql)
	if errQuery != nil {
		return fmt.Errorf("sql error. %v, %v", strSql, errQuery)
	}
	defer rows.Close()

	prtV := reflect.ValueOf(rowPrt).Elem()
	pCount := prtV.NumField()
	vContainer := make([]interface{}, pCount)
	for i := 0; i < pCount; i++ {
		f := prtV.Field(i)
		vContainer[i] = reflect.New(f.Type()).Interface()
	}

	if rows != nil {
		for rows.Next() {
			if err = rows.Scan(vContainer...); err != nil {
				break
			}
			for i := 0; i < pCount; i++ {
				prtV.Field(i).Set(reflect.ValueOf(vContainer[i]).Elem())
			}
			if err = rowCall(rowPrt); err != nil {
				break
			}
		}
	}
	logs.Debug("%v;", strSql)
	return err
}

// 当前连接的数据库表是否存在指定列
func (this *DbCli) HasColumn(tableName string, columnName string) (bool, error) {
	strSql, err := CreateHasColumnSql(this.CurrentDatabase(), tableName, columnName)
	if err != nil {
		logs.Error(fmt.Sprintf("Create sql error: %v", err))
		return false, err
	}
	logs.Debug("%v", strSql)
	var count int
	err = this.db.QueryRow(strSql).Scan(&count)
	if err != nil {
		logs.Error("db exec %v err:%v", strSql, err)
		return false, err
	}
	return count > 0, nil
}

// 执行sql语句
func (this *DbCli) Exec(query string, args ...interface{}) (sql.Result, error) {
	result, err := this.db.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("%v; %v", query, err)
	}
	logs.Debug("%v", query)
	return result, nil
}

// 查询多行数据
func (this *DbCli) QueryRow(query string, args ...interface{}) (*sql.Rows, error) {
	return this.db.Query(query, args...)
}

// 查询单行数据
func (this *DbCli) Query(query string, args ...interface{}) *sql.Row {
	row := this.db.QueryRow(query, args...)
	return row
}

// 查询单行数据, p: 结构实例指针 *mystruct{}, strSql=sql查询脚本
func (this *DbCli) SelectSingleBySql(p interface{}, strSql string) (err error) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		return err
	}
	vContainer := GetValueContainer(schema)
	row := this.Query(strSql)
	if row != nil {
		err = row.Scan(vContainer...)
		if err != nil {
			return err
		}
		err = TransformRowData(schema, vContainer, p)
	}
	logs.Debug("%v;", strSql)
	return err
}

// 查询单行数据, p: 结构实例指针, params: 查询条件(列名-值)  p支持示例： *mystruct{}
func (this *DbCli) SelectSingle(p interface{}, params map[string]interface{}) (err error) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		return err
	}

	var strSql string
	strSql, err = CreateSelectSql(schema, params)
	if err != nil {
		return err
	}

	return this.SelectSingleBySql(p, strSql)
}

// 查询单行数据, p: 结构实例指针, where: 查询条件
func (this *DbCli) SelectSingleByWhere(p interface{}, where string) (err error) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		return err
	}

	var strSql string
	strSql, err = CreateSelectSql(schema, nil)
	if err != nil {
		return err
	}

	strSql = fmt.Sprintf("%v where %v", strSql, where)

	return this.SelectSingleBySql(p, strSql)
}

// 查询多行数据, p: slice或map ,p支持示例： *[]mystruct{}  *[]*mystruct{}  []*mystruct{}  []mystruct{}, sql:sql语句
func (this *DbCli) SelectMultipleBySql(p interface{}, strSql string) (err error) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		return err
	}

	vContainer := GetValueContainer(schema)
	rows, errQuery := this.QueryRow(strSql)
	if errQuery != nil {
		return fmt.Errorf("sql error. %v, %v", strSql, errQuery)
	}
	defer rows.Close()

	if rows != nil {
		structType := GetStructType(reflect.TypeOf(p))
		pContainerKind := reflect.TypeOf(p).Elem().Kind()
		pStructKind := reflect.TypeOf(p).Elem().Elem().Kind()

		results := reflect.ValueOf(p)
		if results.Kind() == reflect.Ptr {
			results = results.Elem()
		}
		for rows.Next() {
			err = rows.Scan(vContainer...)
			if err != nil {
				break
			}
			newItem := reflect.New(structType).Interface()
			err = TransformRowData(schema, vContainer, newItem)
			if err != nil {
				break
			}
			if pContainerKind == reflect.Map {
				firstField := reflect.ValueOf(newItem).Elem().FieldByName(schema.Fields[0].Name)
				if !firstField.IsValid() {
					return fmt.Errorf("%v get value Field '%v' ", reflect.TypeOf(p), schema.Fields[0].Name)
				}
				if pStructKind == reflect.Ptr {
					results.SetMapIndex(firstField, reflect.ValueOf(newItem))
				} else {
					results.SetMapIndex(firstField, reflect.ValueOf(newItem).Elem())
				}
			} else {
				if pStructKind == reflect.Ptr {
					results.Set(reflect.Append(results, reflect.ValueOf(newItem)))
				} else {
					results.Set(reflect.Append(results, reflect.ValueOf(newItem).Elem()))
				}
			}
		}
	}
	logs.Debug("%v;", strSql)
	return err
}

// 查询多行数据, p: slice或map, params: 查询条件(列名-值) p支持示例： *[]mystruct{}  *[]*mystruct{}
func (this *DbCli) SelectMultiple(p interface{}, params map[string]interface{}) (err error) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		return err
	}
	var strSql string
	strSql, err = CreateSelectSql(schema, params)
	if err != nil {
		return err
	}

	return this.SelectMultipleBySql(p, strSql)
}

// 将sql语句放入队列
func (this *DbCli) PutToQueue(strSql string) {
	this.dbQueue.PutToQueue(strSql)
}

// 异步插入数据
func (this *DbCli) AsyncInsert(p interface{}) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		logs.Error(fmt.Sprintf("schema not exists: %v", p))
		return
	}

	arrSql, err := CreateInsertSql(schema, p)
	if err != nil {
		logs.Error(fmt.Sprintf("create sql error: %v", err))
		return
	}

	for _, v := range arrSql {
		this.PutToQueue(v)
	}
}

// 异步更新数据
func (this *DbCli) AsyncUpdate(p interface{}, fields ... string) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		logs.Error(fmt.Sprintf("schema not exists: %v", p))
		return
	}

	strSql, err := CreateUpdateSql(schema, p, fields...)
	if err != nil {
		logs.Error(fmt.Sprintf("create sql error: %v", err))
		return
	}
	this.PutToQueue(strSql)
}

// 异步删除数据
func (this *DbCli) AsyncDelete(p interface{}) {
	schema, err := this.sm.GetSchema(p)
	if err != nil {
		logs.Error(fmt.Sprintf("schema not exists: %v", p))
		return
	}

	strSql, err := CreateDeleteSql(schema, p)
	if err != nil {
		logs.Error(fmt.Sprintf("create sql error: %v", err))
		return
	}
	this.PutToQueue(strSql)
}

// 结构体管理
func (this *DbCli) GetSchemaManager() *SchemaManager {
	return this.sm
}

// 同步表结构
func (this *DbCli) syncTableStruct(hasTablesName []string, schema *Schema) {
	if gutils.ContainSVStr(hasTablesName, schema.TableName) {
		fields, err := this.GetTableStruct(schema.TableName)
		if err != nil {
			logs.Fatal(err)
		}
		addColumnSqls := CreateTableAddColumnSql(schema, fields)
		for _, v := range addColumnSqls {
			_, err = this.Exec(v)
			if err != nil {
				logs.Fatal(err)
			}
		}

		if len(addColumnSqls) > 0 {
			fields, err = this.GetTableStruct(schema.TableName)
			if err != nil {
				logs.Fatal(err)
			}
		}
		modifyColumnSqls := CreateTableModifyColumnSql(schema, fields)
		for _, v := range modifyColumnSqls {
			_, err = this.Exec(v)
			if err != nil {
				logs.Fatal(err)
			}
		}
	} else {
		strSql, err := CreateNewTableSql(schema)
		if err != nil {
			logs.Fatal(errors.New(fmt.Sprintf("sync table struct error: %v", err)))
		}
		_, err = this.Exec(strSql)
		if err != nil {
			logs.Fatal(err)
		}
	}
}

// 同步表结构
func (this *DbCli) SyncAllTableStruct() {
	hasTablesName, err := this.GetAllTableNames()
	if err != nil {
		panic(err)
	}
	for _, v := range this.sm.GetAllSchema() {
		this.syncTableStruct(hasTablesName, v)
	}
}

// 同步表结构
func (this *DbCli) SyncTableStruct(p ... interface{}) {
	if p == nil {
		return
	}

	hasTablesName, err := this.GetAllTableNames()
	if err != nil {
		panic(err)
	}

	for _, v := range p {
		s, err := this.sm.GetSchema(v)
		if err != nil {
			panic(err)
		}
		this.syncTableStruct(hasTablesName, s)
	}
}
