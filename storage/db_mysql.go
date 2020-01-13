package storage

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// 列类型
type EnumColumnType string

const (
	ColumnTypeUnknow   EnumColumnType = "unknow"
	ColumnTypeTinyint  EnumColumnType = "tinyint"
	ColumnTypeSmallint EnumColumnType = "smallint"
	ColumnTypeInt      EnumColumnType = "int"
	ColumnTypeBigint   EnumColumnType = "bigint"
	ColumnTypeFloat    EnumColumnType = "float"
	ColumnTypeDouble   EnumColumnType = "double"
	ColumnTypeVarchar  EnumColumnType = "varchar"
	ColumnTypeDatetime EnumColumnType = "datetime"
)

// 获取列存储类型和长度
func getColumnType(fieldType reflect.Type) (columnType EnumColumnType, columnLength int16, columnDefaultValue string, err error) {
	columnType = ""
	columnLength = 0
	columnDefaultValue = ""
	err = nil
	switch fieldType.Kind() {
	case reflect.Bool:
		columnType = ColumnTypeTinyint
		columnLength = 4
		columnDefaultValue = "0"
		break
	case reflect.Float64:
		columnType = ColumnTypeDouble
		columnLength = 0
		columnDefaultValue = "0"
		break
	case reflect.Float32:
		columnType = ColumnTypeFloat
		columnLength = 0
		columnDefaultValue = "0"
		break
	case reflect.Int8, reflect.Int16:
		columnType = ColumnTypeSmallint
		columnLength = 6
		columnDefaultValue = "0"
		break
	case reflect.Int, reflect.Int32, reflect.Uint32:
		columnType = ColumnTypeInt
		columnLength = 11
		columnDefaultValue = "0"
		break
	case reflect.Int64, reflect.Uint64:
		columnType = ColumnTypeBigint
		columnLength = 20
		columnDefaultValue = "0"
		break
	case reflect.Map, reflect.Struct, reflect.Array, reflect.String, reflect.Slice, reflect.Ptr:
		columnType = ColumnTypeVarchar
		columnLength = 255
		columnDefaultValue = ""
		break
	default:
		columnType = ColumnTypeUnknow
		columnLength = 0
		columnDefaultValue = ""
		err = errors.New(fmt.Sprintf("unknow reflect type [%v] to column type", fieldType))
	}
	return columnType, columnLength, columnDefaultValue, err
}

// 查询数据库名sql语句
func CreateCurrentDatabaseSql() (string, error) {
	return "SELECT DATABASE()", nil
}

// 查询数据库中所有表名sql语句
func CreateSelectTablesName(dbName string) string {
	return fmt.Sprintf("select table_name from information_schema.tables where table_schema='%v'", dbName)
}

// 查询表结构sql语句
func CreateSelectTableStruct(tableName string) string {
	return fmt.Sprintf("desc  %v", tableName)
}

// 是否存在表sql语句
func CreateHasTableSql(dbName string, tableName string) (string, error) {
	return fmt.Sprintf("SELECT count(1) FROM INFORMATION_SCHEMA.tables WHERE table_name = '%v' AND table_schema = '%v'", tableName, dbName), nil
}

// 表是否存在列sql语句
func CreateHasColumnSql(dbName string, tableName string, columnName string) (string, error) {
	return fmt.Sprintf("SELECT count(1) FROM information_schema.columns WHERE table_schema = '%v' AND table_name = '%v' AND column_name = '%v'", dbName, tableName, columnName), nil
}

// 修改表名
func CreateAlterTableNameSql(oldTableName string, newTableName string) string {
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldTableName, newTableName)
}

// 表中列的最大值
func CreateColumnMaxValueSql(tableName string, columnName string) (string, error) {
	return fmt.Sprintf("SELECT IFNULL(MAX(%v),0) FROM %v", columnName, tableName), nil
}

// 创建表sql语句, schema:结构体概要信息
func CreateNewTableSql(schema *Schema) (string, error) {
	var buf bytes.Buffer
	primaryKeys := make([]string, 0)
	buf.WriteString(fmt.Sprintf("CREATE TABLE `%v` (", schema.TableName))
	for _, v := range schema.Fields {
		// 列SQL
		buf.WriteString(fmt.Sprintf("%v,", getColumnSql(schema, v)))
		// 主键
		if v.PrimaryKey {
			primaryKeys = append(primaryKeys, fmt.Sprintf("`%v`", v.ColumnName))
		}
	}

	// 主键
	if len(primaryKeys) == 0 {
		return "", errors.New("table must have a need primary key")
	}
	buf.WriteString(fmt.Sprintf(" PRIMARY KEY (%v)", strings.Join(primaryKeys, ",")))

	// 索引
	if schema.IndexKeys != nil && len(schema.IndexKeys) > 0 {
		for _, v := range schema.IndexKeys {
			if len(v) > 0 {
				arr := make([]string, 0)
				for _, name := range v {
					arr = append(arr, fmt.Sprintf("`%v`", name))
				}
				buf.WriteString(fmt.Sprintf(",KEY `idx_%v` (%v)", strings.ToLower(strings.Join(v, "_")), strings.ToLower(strings.Join(arr, ","))))
			}
		}
	}

	buf.WriteString(") ENGINE=MyISAM DEFAULT CHARSET=utf8;")

	return buf.String(), nil
}

// 获取列构造SQL
func getColumnSql(schema *Schema, field *Field) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf(" `%v` %v", field.ColumnName, field.ColumnType))
	// 列长度
	if field.ColumnLength > 0 {
		buf.WriteString(fmt.Sprintf("(%v)", field.ColumnLength))
	}
	// 列允许空
	if !field.ColumnNull {
		buf.WriteString(" NOT NULL")
	}
	// 列自增
	if field.AutoIncrement {
		buf.WriteString(" AUTO_INCREMENT")
	} else {
		// 列默认值
		if field.ColumnType != ColumnTypeDatetime {
			buf.WriteString(fmt.Sprintf(" DEFAULT '%v'", field.ColumnDefaultValue))
		}
	}

	return buf.String()
}

// 增加表列sql语句, schema:结构体概要信息, field:列信息
func CreateNewColumnSql(schema *Schema, field *Field) string {
	return fmt.Sprintf("ALTER TABLE `%v` ADD COLUMN %v;", schema.TableName, getColumnSql(schema, field))
}

// 修改表列sql语句, schema:结构体概要信息, field:列信息
func CreateModifyColumnSql(schema *Schema, field *Field) string {
	return fmt.Sprintf("ALTER TABLE `%v` MODIFY COLUMN %v;", schema.TableName, getColumnSql(schema, field))
}

// 创建分表sql语句(旧表改名, 创建新表)
func CreateSeparateTableSql(schema *Schema, separateTableName string) ([]string, error) {
	arrSql := make([]string, 0, 2)

	// 原表名称改成分表的名称
	strSqlAlter := CreateAlterTableNameSql(schema.TableName, separateTableName)
	arrSql = append(arrSql, strSqlAlter)

	// 重新创建当前使用的表
	strSqlCreate, err := CreateNewTableSql(schema)
	if err != nil {
		return nil, err
	}
	arrSql = append(arrSql, strSqlCreate)

	return arrSql, nil
}

// 插入表sql语句, p:结构体引用
func CreateInsertSql(schema *Schema, p interface{}) (arrSql []string, err error) {
	arrSql = make([]string, 0, 1)

	// 获取表名(如果有分表, 需要处理分表表名)
	isSeparate, separateTableName := schema.GetSeparateTableName()
	if isSeparate {
		arrSeparateSql, err := CreateSeparateTableSql(schema, separateTableName)
		if err != nil {
			return nil, err
		}
		arrSql = append(arrSql, arrSeparateSql...)
	}

	var buf bytes.Buffer

	rv := reflect.ValueOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	k := make([]string, 0)
	v := make([]string, 0)
	var cv interface{}
	for _, field := range schema.Fields {
		k = append(k, fmt.Sprintf("`%v`", field.ColumnName))
		cv, err = ParseColumnValue(field, rv.FieldByName(field.Name).Interface())
		if err != nil {
			return nil, err
		}
		v = append(v, fmt.Sprintf("'%v'", cv))
	}

	buf.WriteString("INSERT INTO `")
	buf.WriteString(schema.TableName)
	buf.WriteString("`(")
	buf.WriteString(strings.Join(k, ","))
	buf.WriteString(")  VALUES(")
	buf.WriteString(strings.Join(v, ","))
	buf.WriteString(")")

	arrSql = append(arrSql, buf.String())

	return arrSql, err
}

// 更新表sql语句(条件默认列id), p:结构体引用
func CreateUpdateSql(schema *Schema, p interface{}, fields ... string) (strSql string, err error) {
	rv := reflect.ValueOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	var buf bytes.Buffer

	// 更新的表
	buf.WriteString("UPDATE `")
	buf.WriteString(schema.TableName)
	buf.WriteString("`")

	// 更新的字段
	buf.WriteString(" SET ")
	flag := true

	updateFields := make([]*Field, 0)
	if len(fields) == 0 {
		updateFields = schema.Fields
	} else {
		for _, v := range fields {
			field := schema.GetField(v)
			if field == nil {
				return "", errors.New(fmt.Sprintf("field not exists: %v", v))
			}
			updateFields = append(updateFields, field)
		}
	}

	var cv interface{}
	for _, field := range updateFields {
		if field.PrimaryKey == false {
			cv, err = ParseColumnValue(field, rv.FieldByName(field.Name).Interface())
			if err != nil {
				return "", err
			}
			if !flag {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("`%v`='%v'", field.ColumnName, cv))
			flag = false
		}
	}

	// 更新的条件
	buf.WriteString(" WHERE ")
	flag = true
	for _, field := range schema.Fields {
		if field.PrimaryKey {
			if !flag {
				buf.WriteString(" AND ")
			}
			buf.WriteString(fmt.Sprintf("`%v`='%v'", field.ColumnName, rv.FieldByName(field.Name).Interface()))
			flag = false
		}
	}

	return buf.String(), err
}

// 删除表数据sql语句, schema:结构体信息, p:结构体引用
func CreateDeleteSql(schema *Schema, p interface{}) (strSql string, err error) {
	rv := reflect.ValueOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	var buf bytes.Buffer

	// 删除的表
	buf.WriteString("DELETE FROM `")
	buf.WriteString(schema.TableName)
	buf.WriteString("`")

	// 删除的条件
	buf.WriteString(" WHERE ")
	flag := true
	for _, field := range schema.Fields {
		if field.PrimaryKey {
			if !flag {
				buf.WriteString(" AND ")
			}
			buf.WriteString(fmt.Sprintf("`%v`='%v'", field.ColumnName, rv.FieldByName(field.Name).Interface()))
			flag = false
		}
	}

	return buf.String(), err
}

// 查询表数据sql语句, schema:结构体信息, args:k-v(列名,值)条件
func CreateSelectSql(schema *Schema, params map[string]interface{}) (strSql string, err error) {
	var buf bytes.Buffer

	buf.WriteString("SELECT ")
	for idx, field := range schema.Fields {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("`%v`", field.ColumnName))
	}
	buf.WriteString(fmt.Sprintf(" FROM `%v`", schema.TableName))

	if params != nil && len(params) > 0 {
		buf.WriteString(" WHERE ")
		flag := true
		for k, v := range params {
			if !flag {
				buf.WriteString(" AND ")
			}
			buf.WriteString(fmt.Sprintf("`%v`='%v'", k, v))
			flag = false
		}
	}
	return buf.String(), err
}

// 获取新增字段sql语句
func CreateTableAddColumnSql(schema *Schema, fields []*Field) []string {
	changleSqls := make([]string, 0)

	exists := false
	for i, newV := range schema.Fields {
		exists = false
		for _, oldV := range fields {
			if oldV.ColumnName == newV.ColumnName {
				exists = true
				break
			}
		}
		if !exists {
			strSql := CreateNewColumnSql(schema, newV)
			if i > 0 {
				strSql = fmt.Sprintf("%v AFTER `%v`;", strings.TrimRight(strSql, ";"), schema.Fields[i-1].ColumnName)
			}
			changleSqls = append(changleSqls, strSql)
		}
	}
	return changleSqls
}

// 获取修改字段sql语句
func CreateTableModifyColumnSql(schema *Schema, fields []*Field) []string {
	changeSqls := make([]string, 0)

	for i, newV := range schema.Fields {
		for j, oldV := range fields {
			if oldV.ColumnName == newV.ColumnName {
				if i != j || oldV.ColumnType != newV.ColumnType || oldV.ColumnLength != newV.ColumnLength {
					strSql := CreateModifyColumnSql(schema, newV)
					if i > 0 {
						strSql = fmt.Sprintf("%v AFTER `%v`;", strings.TrimRight(strSql, ";"), schema.Fields[i-1].ColumnName)
					}
					changeSqls = append(changeSqls, strSql)
				}
				break
			}
		}
	}

	return changeSqls
}

// 替换db敏感字符
func escapeBackslash(v []byte) []byte {
	pos := 0
	buf := make([]byte, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}
