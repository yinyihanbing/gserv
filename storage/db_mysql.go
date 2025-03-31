package storage

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// EnumColumnType represents the column type in the database.
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

// getColumnType determines the column type, length, and default value based on the Go type.
// Returns an error if the type is unsupported.
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
	case reflect.Float64:
		columnType = ColumnTypeDouble
		columnLength = 0
		columnDefaultValue = "0"
	case reflect.Float32:
		columnType = ColumnTypeFloat
		columnLength = 0
		columnDefaultValue = "0"
	case reflect.Int8, reflect.Int16:
		columnType = ColumnTypeSmallint
		columnLength = 6
		columnDefaultValue = "0"
	case reflect.Int, reflect.Int32, reflect.Uint32:
		columnType = ColumnTypeInt
		columnLength = 11
		columnDefaultValue = "0"
	case reflect.Int64, reflect.Uint64:
		columnType = ColumnTypeBigint
		columnLength = 20
		columnDefaultValue = "0"
	case reflect.Map, reflect.Struct, reflect.Array, reflect.String, reflect.Slice, reflect.Ptr:
		columnType = ColumnTypeVarchar
		columnLength = 255
		columnDefaultValue = ""
	default:
		columnType = ColumnTypeUnknow
		columnLength = 0
		columnDefaultValue = ""
		err = fmt.Errorf("unknown reflect type [%v] to column type", fieldType)
	}
	return columnType, columnLength, columnDefaultValue, err
}

// CreateCurrentDatabaseSql generates the SQL query to get the current database name.
func CreateCurrentDatabaseSql() (string, error) {
	return "SELECT DATABASE()", nil
}

// CreateSelectTablesName generates the SQL query to list all table names in a database.
func CreateSelectTablesName(dbName string) string {
	return fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='%v'", dbName)
}

// CreateSelectTableStruct generates the SQL query to describe the structure of a table.
func CreateSelectTableStruct(tableName string) string {
	return fmt.Sprintf("DESC %v", tableName)
}

// CreateHasTableSql generates the SQL query to check if a table exists in a database.
func CreateHasTableSql(dbName string, tableName string) (string, error) {
	return fmt.Sprintf("SELECT COUNT(1) FROM information_schema.tables WHERE table_name = '%v' AND table_schema = '%v'", tableName, dbName), nil
}

// CreateHasColumnSql generates the SQL query to check if a column exists in a table.
func CreateHasColumnSql(dbName string, tableName string, columnName string) (string, error) {
	return fmt.Sprintf("SELECT COUNT(1) FROM information_schema.columns WHERE table_schema = '%v' AND table_name = '%v' AND column_name = '%v'", dbName, tableName, columnName), nil
}

// CreateAlterTableNameSql generates the SQL query to rename a table.
func CreateAlterTableNameSql(oldTableName string, newTableName string) string {
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldTableName, newTableName)
}

// CreateColumnMaxValueSql generates the SQL query to get the maximum value of a column in a table.
func CreateColumnMaxValueSql(tableName string, columnName string) (string, error) {
	return fmt.Sprintf("SELECT IFNULL(MAX(%v), 0) FROM %v", columnName, tableName), nil
}

// CreateNewTableSql generates the SQL query to create a new table based on the schema.
func CreateNewTableSql(schema *Schema) (string, error) {
	return CreateNewTableSqlWithTableName(schema, schema.TableName)
}

// CreateNewTableSqlWithTableName generates the SQL query to create a new table with a specified name.
func CreateNewTableSqlWithTableName(schema *Schema, tableName string) (string, error) {
	var buf bytes.Buffer
	primaryKeys := make([]string, 0)

	buf.WriteString(fmt.Sprintf("CREATE TABLE `%v` (", tableName))
	for _, v := range schema.Fields {
		// Add column SQL
		buf.WriteString(fmt.Sprintf("%v,", getColumnSql(schema, v)))
		// Collect primary keys
		if v.PrimaryKey {
			primaryKeys = append(primaryKeys, fmt.Sprintf("`%v`", v.ColumnName))
		}
	}

	// Ensure at least one primary key exists
	if len(primaryKeys) == 0 {
		return "", errors.New("table must have at least one primary key")
	}
	buf.WriteString(fmt.Sprintf(" PRIMARY KEY (%v)", strings.Join(primaryKeys, ",")))

	// Add indexes if any
	if len(schema.IndexKeys) > 0 {
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

// getColumnSql generates the SQL definition for a column based on its schema and field properties.
func getColumnSql(schema *Schema, field *Field) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf(" `%v` %v", field.ColumnName, field.ColumnType))
	// Add column length if specified
	if field.ColumnLength > 0 {
		buf.WriteString(fmt.Sprintf("(%v)", field.ColumnLength))
	}
	// Specify if the column allows NULL values
	if !field.ColumnNull {
		buf.WriteString(" NOT NULL")
	}
	// Add auto-increment property if applicable
	if field.AutoIncrement {
		buf.WriteString(" AUTO_INCREMENT")
	} else {
		// Add default value if not a datetime column
		if field.ColumnType != ColumnTypeDatetime {
			buf.WriteString(fmt.Sprintf(" DEFAULT '%v'", field.ColumnDefaultValue))
		}
	}

	return buf.String()
}

// CreateNewColumnSql generates the SQL query to add a new column to a table.
func CreateNewColumnSql(schema *Schema, field *Field) string {
	return fmt.Sprintf("ALTER TABLE `%v` ADD COLUMN %v;", schema.TableName, getColumnSql(schema, field))
}

// CreateModifyColumnSql generates the SQL query to modify an existing column in a table.
func CreateModifyColumnSql(schema *Schema, field *Field) string {
	return fmt.Sprintf("ALTER TABLE `%v` MODIFY COLUMN %v;", schema.TableName, getColumnSql(schema, field))
}

// CreateSeparateTableSql generates the SQL queries to rename an existing table and create a new table based on the schema.
func CreateSeparateTableSql(schema *Schema, separateTableName string) ([]string, error) {
	arrSql := make([]string, 0, 2)

	// Rename the original table to the separate table name
	strSqlAlter := CreateAlterTableNameSql(schema.TableName, separateTableName)
	arrSql = append(arrSql, strSqlAlter)

	// Create a new table with the original table name
	strSqlCreate, err := CreateNewTableSql(schema)
	if err != nil {
		return nil, err
	}
	arrSql = append(arrSql, strSqlCreate)

	return arrSql, nil
}

// CreateInsertSql generates the SQL query to insert a new row into a table based on the schema and the provided struct.
func CreateInsertSql(schema *Schema, p any) (arrSql []string, err error) {
	arrSql = make([]string, 0, 1)

	// Get the table name (handle separate tables if applicable)
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
	var cv any
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
	buf.WriteString(") VALUES(")
	buf.WriteString(strings.Join(v, ","))
	buf.WriteString(")")

	arrSql = append(arrSql, buf.String())

	return arrSql, err
}

// CreateUpdateSql generates the SQL query to update a row in a table based on the schema and the provided struct.
// The update is performed based on the primary key columns.
func CreateUpdateSql(schema *Schema, p any, fields ...string) (strSql string, err error) {
	rv := reflect.ValueOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	var buf bytes.Buffer

	// Specify the table to update
	buf.WriteString("UPDATE `")
	buf.WriteString(schema.TableName)
	buf.WriteString("`")

	// Specify the columns to update
	buf.WriteString(" SET ")
	flag := true

	updateFields := make([]*Field, 0)
	if len(fields) == 0 {
		updateFields = schema.Fields
	} else {
		for _, v := range fields {
			field := schema.GetField(v)
			if field == nil {
				return "", fmt.Errorf("field not exists: %v", v)
			}
			updateFields = append(updateFields, field)
		}
	}

	var cv any
	for _, field := range updateFields {
		if !field.PrimaryKey {
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

	// Specify the conditions for the update (based on primary key columns)
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

// CreateDeleteSql generates the SQL query to delete a row from a table based on the schema and the provided struct.
// The deletion is performed based on the primary key columns.
func CreateDeleteSql(schema *Schema, p any) (strSql string, err error) {
	rv := reflect.ValueOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	var buf bytes.Buffer

	// Specify the table to delete from
	buf.WriteString("DELETE FROM `")
	buf.WriteString(schema.TableName)
	buf.WriteString("`")

	// Specify the conditions for the deletion (based on primary key columns)
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

// CreateSelectSql generates the SQL query to select rows from a table based on the schema and the provided conditions.
func CreateSelectSql(schema *Schema, params map[string]any) (strSql string, err error) {
	var buf bytes.Buffer

	buf.WriteString("SELECT ")
	for idx, field := range schema.Fields {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("`%v`", field.ColumnName))
	}
	buf.WriteString(fmt.Sprintf(" FROM `%v`", schema.TableName))

	// Check if params has conditions
	if len(params) > 0 {
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

// CreateTableAddColumnSql generates the SQL queries to add new columns to a table based on the schema.
func CreateTableAddColumnSql(schema *Schema, fields []*Field) []string {
	changeSqls := make([]string, 0)

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
			changeSqls = append(changeSqls, strSql)
		}
	}
	return changeSqls
}

// CreateTableModifyColumnSql generates the SQL queries to modify existing columns in a table based on the schema.
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

// escapeBackslash replaces sensitive characters in a byte slice with their escaped equivalents.
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
