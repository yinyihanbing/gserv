package storage

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yinyihanbing/gutils"
	"github.com/yinyihanbing/gutils/logs"
)

// DbCli represents a database client with configuration, connection pool, and schema manager.
type DbCli struct {
	config  *DbConfig
	db      *sql.DB
	sm      *SchemaManager
	dbQueue *DbQueue
	DbName  string
}

// DbConfig holds the configuration for database connection.
type DbConfig struct {
	StrAddr         string
	ConnMaxLifetime time.Duration
	MaxOpenConns    int
	MaxIdleConns    int

	QueueType        DbQueueType
	QueueRedisCliIdx int
	QueueDbCliIdx    int
	QueueLimitCount  int
}

// newDbCli initializes a new database client with the given configuration.
// returns the database client or an error if the connection fails.
func newDbCli(cfg *DbConfig) (db *DbCli, err error) {
	var d *sql.DB
	d, err = sql.Open("mysql", cfg.StrAddr)
	if err != nil {
		logs.Error("mysql connection failed: %v %v", cfg.StrAddr, err)
		return nil, fmt.Errorf("mysql connection failed: %v", err)
	}
	if err = d.Ping(); err != nil {
		d.Close()
		logs.Error("mysql connection failed: %v %v", cfg.StrAddr, err)
		return nil, fmt.Errorf("mysql connection failed: %v", err)
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

	db.sm = newSchemaManager()

	db.dbQueue = NewDbQueue(cfg.QueueType, cfg.QueueRedisCliIdx, cfg.QueueDbCliIdx, cfg.QueueLimitCount)

	db.DbName = db.CurrentDatabase()

	logs.Info("mysql connection success: %v", cfg.StrAddr)

	return db, nil
}

// Destroy closes the database connection and destroys the queue.
func (dc *DbCli) Destroy() {
	dc.dbQueue.Destroy()

	if dc.db != nil {
		dc.db.Close()
	}
}

// StartQueue starts the database queue task.
func (dc *DbCli) StartQueue() {
	dc.dbQueue.StartQueueTask()
}

// CurrentDatabase retrieves the name of the currently connected database.
func (dc *DbCli) CurrentDatabase() (name string) {
	strSql, err := CreateCurrentDatabaseSql()
	if err != nil {
		logs.Error("create sql error: %v", err)
		return
	}
	dc.db.QueryRow(strSql).Scan(&name)
	return
}

// GetAllTableNames retrieves all table names from the current database.
// returns a slice of table names or an error.
func (dc *DbCli) GetAllTableNames() ([]string, error) {
	strSql := CreateSelectTablesName(dc.CurrentDatabase())
	logs.Debug("%v", strSql)

	rows, err := dc.QueryRow(strSql)
	if err != nil {
		return nil, fmt.Errorf("sql error: %v, %v", strSql, err)
	}
	defer rows.Close()

	tableNames := make([]string, 0)
	for rows.Next() {
		var col1 string
		err = rows.Scan(&col1)
		if err != nil {
			return nil, fmt.Errorf("sql error: %v, %v", strSql, err)
		}
		tableNames = append(tableNames, col1)
	}

	return tableNames, nil
}

// GetTableStruct retrieves the structure of a specific table.
// returns a slice of Field or an error.
func (dc *DbCli) GetTableStruct(tableName string) ([]*Field, error) {
	strSql := CreateSelectTableStruct(tableName)
	logs.Debug("%v", strSql)

	rows, err := dc.QueryRow(strSql)
	if err != nil {
		return nil, fmt.Errorf("sql error: %v, %v", strSql, err)
	}
	defer rows.Close()

	fields := make([]*Field, 0)
	for rows.Next() {
		var col1 string
		var col2 string
		var col3 string
		var col4 string
		var col5 any
		var col6 any
		err = rows.Scan(&col1, &col2, &col3, &col4, &col5, &col6)
		if err != nil {
			return nil, fmt.Errorf("sql error: %v, %v", strSql, err)
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

// HasTable checks if a specific table exists in the current database.
// returns true if the table exists, otherwise false.
func (dc *DbCli) HasTable(tableName string) (bool, error) {
	strSql, err := CreateHasTableSql(dc.CurrentDatabase(), tableName)
	if err != nil {
		logs.Error("create sql error: %v", err)
		return false, err
	}
	logs.Debug("%v", strSql)
	var count int
	err = dc.db.QueryRow(strSql).Scan(&count)
	if err != nil {
		logs.Error("db exec %v err: %v", strSql, err)
		return false, fmt.Errorf("db execution error: %v", err)
	}
	return count > 0, nil
}

// SelectMaxValue retrieves the maximum value of a specific column in a table.
// returns the maximum value or an error.
func (dc *DbCli) SelectMaxValue(tableName string, columnName string) (int64, error) {
	strSql, err := CreateColumnMaxValueSql(tableName, columnName)
	if err != nil {
		logs.Error("create sql error: %v", err)
		return 0, err
	}
	logs.Debug("%v", strSql)
	var count int64
	err = dc.db.QueryRow(strSql).Scan(&count)
	if err != nil {
		logs.Error("db exec %v err: %v", strSql, err)
		return 0, fmt.Errorf("db execution error: %v", err)
	}
	return count, nil
}

// SelectRowScanBySql executes a query and scans multiple rows into a provided structure.
// rowCall is a callback function to process each row.
func (dc *DbCli) SelectRowScanBySql(strSql string, rowPrt any, rowCall func(rowPrt any) error) (err error) {
	rows, errQuery := dc.QueryRow(strSql)
	if errQuery != nil {
		return fmt.Errorf("sql error: %v, %v", strSql, errQuery)
	}
	defer rows.Close()

	prtV := reflect.ValueOf(rowPrt).Elem()
	pCount := prtV.NumField()
	vContainer := make([]any, pCount)
	for i := range pCount {
		f := prtV.Field(i)
		vContainer[i] = reflect.New(f.Type()).Interface()
	}

	if rows != nil {
		for rows.Next() {
			if err = rows.Scan(vContainer...); err != nil {
				break
			}
			for i := range pCount {
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

// HasColumn checks if a specific column exists in a table.
// returns true if the column exists, otherwise false.
func (dc *DbCli) HasColumn(tableName string, columnName string) (bool, error) {
	strSql, err := CreateHasColumnSql(dc.CurrentDatabase(), tableName, columnName)
	if err != nil {
		logs.Error("create sql error: %v", err)
		return false, err
	}
	logs.Debug("%v", strSql)
	var count int
	err = dc.db.QueryRow(strSql).Scan(&count)
	if err != nil {
		logs.Error("db exec %v err: %v", strSql, err)
		return false, fmt.Errorf("db execution error: %v", err)
	}
	return count > 0, nil
}

// Exec executes a SQL query with optional arguments.
// returns the result or an error.
func (dc *DbCli) Exec(query string, args ...any) (sql.Result, error) {
	result, err := dc.db.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("execution error: %v; %v", query, err)
	}
	logs.Debug("%v", query)
	return result, nil
}

// QueryRow executes a query and returns multiple rows.
func (dc *DbCli) QueryRow(query string, args ...any) (*sql.Rows, error) {
	return dc.db.Query(query, args...)
}

// Query executes a query and returns a single row.
func (dc *DbCli) Query(query string, args ...any) *sql.Row {
	row := dc.db.QueryRow(query, args...)
	return row
}

// SelectSingleBySql retrieves a single row based on a SQL query and maps it to the provided structure.
func (dc *DbCli) SelectSingleBySql(p any, strSql string) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}
	vContainer := GetValueContainer(schema)
	row := dc.Query(strSql)
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

// SelectSingle retrieves a single row based on query parameters and maps it to the provided structure.
func (dc *DbCli) SelectSingle(p any, params map[string]any) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}

	var strSql string
	strSql, err = CreateSelectSql(schema, params)
	if err != nil {
		return err
	}

	return dc.SelectSingleBySql(p, strSql)
}

// SelectSingleByWhere retrieves a single row based on a where clause and maps it to the provided structure.
func (dc *DbCli) SelectSingleByWhere(p any, where string) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}

	var strSql string
	strSql, err = CreateSelectSql(schema, nil)
	if err != nil {
		return err
	}

	strSql = fmt.Sprintf("%v where %v", strSql, where)

	return dc.SelectSingleBySql(p, strSql)
}

// SelectMultipleBySql retrieves multiple rows based on a SQL query and maps them to the provided structure.
func (dc *DbCli) SelectMultipleBySql(p any, strSql string) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}

	vContainer := GetValueContainer(schema)
	rows, errQuery := dc.QueryRow(strSql)
	if errQuery != nil {
		return fmt.Errorf("sql error: %v, %v", strSql, errQuery)
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

// SelectMultiple retrieves multiple rows based on query parameters and maps them to the provided structure.
func (dc *DbCli) SelectMultiple(p any, params map[string]any) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}
	var strSql string
	strSql, err = CreateSelectSql(schema, params)
	if err != nil {
		return err
	}

	return dc.SelectMultipleBySql(p, strSql)
}

// SelectScan iterates over multiple rows and processes each row using the provided callback function.
func (dc *DbCli) SelectScan(p any, params map[string]any, iterFunc func(v any, err error) bool) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}
	var strSql string
	strSql, err = CreateSelectSql(schema, params)
	if err != nil {
		return err
	}

	return dc.SelectScanBySql(p, strSql, iterFunc)
}

// SelectScanBySql iterates over multiple rows based on a SQL query and processes each row using the provided callback function.
func (dc *DbCli) SelectScanBySql(p any, strSql string, iterFunc func(v any, err error) bool) (err error) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		return err
	}

	vContainer := GetValueContainer(schema)
	rows, errQuery := dc.QueryRow(strSql)
	if errQuery != nil {
		return fmt.Errorf("sql error: %v, %v", strSql, errQuery)
	}
	defer rows.Close()

	if rows != nil {
		for rows.Next() {
			err = rows.Scan(vContainer...)
			if err != nil {
				iterFunc(nil, err)
				break
			}
			err = TransformRowData(schema, vContainer, p)
			if err != nil {
				iterFunc(nil, err)
				break
			}
			if !iterFunc(p, err) {
				break
			}
		}
	}
	logs.Debug("%v;", strSql)
	return err
}

// PutToQueue adds a SQL query to the database queue for asynchronous execution.
func (dc *DbCli) PutToQueue(strSql string) {
	dc.dbQueue.PutToQueue(strSql)
}

// AsyncInsert inserts data asynchronously into the database.
func (dc *DbCli) AsyncInsert(p any) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		logs.Error("schema not exists: %v", p)
		return
	}

	arrSql, err := CreateInsertSql(schema, p)
	if err != nil {
		logs.Error("create sql error: %v", err)
		return
	}

	for _, v := range arrSql {
		dc.PutToQueue(v)
	}
}

// AsyncUpdate updates data asynchronously in the database.
func (dc *DbCli) AsyncUpdate(p any, fields ...string) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		logs.Error("schema not exists: %v", p)
		return
	}

	strSql, err := CreateUpdateSql(schema, p, fields...)
	if err != nil {
		logs.Error("create sql error: %v", err)
		return
	}
	dc.PutToQueue(strSql)
}

// AsyncDelete deletes data asynchronously from the database.
func (dc *DbCli) AsyncDelete(p any) {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		logs.Error("schema not exists: %v", p)
		return
	}

	strSql, err := CreateDeleteSql(schema, p)
	if err != nil {
		logs.Error("create sql error: %v", err)
		return
	}
	dc.PutToQueue(strSql)
}

// GetSchemaManager retrieves the schema manager associated with the database client.
func (dc *DbCli) GetSchemaManager() *SchemaManager {
	return dc.sm
}

// syncTableStruct synchronizes the structure of a table with the database schema.
func (dc *DbCli) syncTableStruct(hasTablesName []string, schema *Schema) {
	if gutils.ContainSVStr(hasTablesName, schema.TableName) {
		fields, err := dc.GetTableStruct(schema.TableName)
		if err != nil {
			logs.Fatal(err)
		}
		addColumnSqls := CreateTableAddColumnSql(schema, fields)
		for _, v := range addColumnSqls {
			_, err = dc.Exec(v)
			if err != nil {
				logs.Fatal(err)
			}
		}

		if len(addColumnSqls) > 0 {
			fields, err = dc.GetTableStruct(schema.TableName)
			if err != nil {
				logs.Fatal(err)
			}
		}
		modifyColumnSqls := CreateTableModifyColumnSql(schema, fields)
		for _, v := range modifyColumnSqls {
			_, err = dc.Exec(v)
			if err != nil {
				logs.Fatal(err)
			}
		}
	} else {
		strSql, err := CreateNewTableSql(schema)
		if err != nil {
			logs.Fatal(fmt.Errorf("sync table struct error: %v", err))
		}
		_, err = dc.Exec(strSql)
		if err != nil {
			logs.Fatal(err)
		}
	}
}

// SyncAllTableStruct synchronizes the structure of all tables with the database schema.
func (dc *DbCli) SyncAllTableStruct() {
	hasTablesName, err := dc.GetAllTableNames()
	if err != nil {
		panic(err)
	}
	for _, v := range dc.sm.GetAllSchema() {
		dc.syncTableStruct(hasTablesName, v)
	}
}

// SyncTableStruct synchronizes the structure of specific tables with the database schema.
func (dc *DbCli) SyncTableStruct(p ...any) {
	if p == nil {
		return
	}

	hasTablesName, err := dc.GetAllTableNames()
	if err != nil {
		panic(err)
	}

	for _, v := range p {
		s, err := dc.sm.GetSchema(v)
		if err != nil {
			panic(err)
		}
		dc.syncTableStruct(hasTablesName, s)
	}
}

// CheckCreateSeparateTable checks and creates a separate table asynchronously if needed.
func (dc *DbCli) CheckCreateSeparateTable(p any) error {
	return dc.CreateSeparateTable(p, true)
}

// CreateSeparateTable creates a separate table synchronously or asynchronously based on the async flag.
func (dc *DbCli) CreateSeparateTable(p any, async bool) error {
	schema, err := dc.sm.GetSchema(p)
	if err != nil {
		logs.Error("schema not exists: %v", p)
		return err
	}

	isSeparate, separateTableName := schema.GetSeparateTableName()
	if !isSeparate {
		return nil
	}

	arrSeparateSql, err := CreateSeparateTableSql(schema, separateTableName)
	if err != nil {
		return err
	}
	if len(arrSeparateSql) == 0 {
		return nil
	}
	for _, v := range arrSeparateSql {
		if async {
			dc.PutToQueue(v)
		} else {
			if _, err := dc.Exec(v); err != nil {
				return err
			}
		}
	}
	return nil
}
