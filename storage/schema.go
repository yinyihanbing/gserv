package storage

import (
	"fmt"
	"go/ast"
	"reflect"
	"time"

	"github.com/yinyihanbing/gutils/logs"
)

type SchemaManager struct {
	schemas map[reflect.Type]*Schema
}

// Schema represents the structure information of a database table.
type Schema struct {
	Type          reflect.Type
	TableName     string
	Fields        []*Field
	IndexKeys     [][]string
	separateTable *SeparateTable // configuration for table sharding (nil if no sharding)
}

// Field represents the metadata of a database column.
type Field struct {
	Name               string
	Type               reflect.Type
	ColumnName         string         // column name
	ColumnType         EnumColumnType // column type
	ColumnLength       int16          // length
	ColumnNull         bool           // nullable
	ColumnDefaultValue string         // default value
	PrimaryKey         bool           // primary key
	AutoIncrement      bool           // auto-increment
}

// newSchemaManager initializes a new SchemaManager instance.
func newSchemaManager() *SchemaManager {
	s := &SchemaManager{}
	s.schemas = map[reflect.Type]*Schema{}
	return s
}

// Register registers the schema of a struct. pks: primary key field names.
func (s *SchemaManager) Register(p interface{}, pks ...string) *Schema {
	reflectType := GetStructType(reflect.TypeOf(p))

	schema := &Schema{Type: reflectType, TableName: ChangleName(reflectType.Name())}
	var err error
	var cName string
	var cType EnumColumnType
	var cLength int16
	var cDefaultValue string
	num := reflectType.NumField()
	for i := 0; i < num; i++ {
		if fieldStruct := reflectType.Field(i); ast.IsExported(fieldStruct.Name) {
			cName = ChangleName(fieldStruct.Name)
			cType, cLength, cDefaultValue, err = getColumnType(fieldStruct.Type)
			if err != nil {
				panic(fmt.Errorf("register schema error: struct %v, error %v", reflectType.Name(), err))
			}
			schema.Fields = append(schema.Fields, &Field{
				Name:               fieldStruct.Name,
				Type:               fieldStruct.Type,
				ColumnName:         cName,
				ColumnType:         cType,
				ColumnLength:       cLength,
				ColumnDefaultValue: cDefaultValue,
				PrimaryKey:         false,
			})
		}
	}
	// set primary keys
	schema.setTablePrimaryKeys(pks...)

	s.schemas[schema.Type] = schema
	logs.Debug("schema register success: %v", schema.Type)

	return schema
}

// GetAllSchema returns all registered schemas.
func (s *SchemaManager) GetAllSchema() map[reflect.Type]*Schema {
	return s.schemas
}

// GetSchema retrieves the schema of a given struct type.
func (s *SchemaManager) GetSchema(p interface{}) (*Schema, error) {
	reflectType := GetStructType(reflect.TypeOf(p))
	if v, ok := s.schemas[reflectType]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("schema does not exist: %v", reflectType)
}

// GetSeparateTableName returns whether the table is sharded and the sharded table name.
func (s *Schema) GetSeparateTableName() (isSeparate bool, separateTableName string) {
	if s.separateTable == nil {
		return false, ""
	}
	return s.separateTable.IsNowSeparate()
}

// GetField finds a field by its name.
func (s *Schema) GetField(field string) *Field {
	for _, f := range s.Fields {
		if f.Name == field {
			return f
		}
	}
	return nil
}

// setTablePrimaryKeys sets the primary keys for the table and adds indexes for them.
func (s *Schema) setTablePrimaryKeys(fields ...string) {
	if len(fields) == 0 {
		return
	}
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(fmt.Errorf("field does not exist: '%v'", v))
		}
		f.PrimaryKey = true
	}
	s.AddTableIdx(fields...)
}

// AddTableIdx adds an index to the table for the specified fields.
func (s *Schema) AddTableIdx(fields ...string) *Schema {
	if len(fields) == 0 {
		return s
	}
	clo := make([]string, 0)
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(fmt.Errorf("field does not exist: '%v'", v))
		}
		clo = append(clo, f.ColumnName)
	}
	s.IndexKeys = append(s.IndexKeys, clo)

	return s
}

// SetDateTimeColumnType sets the column type to datetime for the specified fields.
func (s *Schema) SetDateTimeColumnType(fields ...string) *Schema {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(fmt.Errorf("field does not exist: '%v'", v))
		}
		f.ColumnType = ColumnTypeDatetime
		f.ColumnLength = 0
	}

	return s
}

// SetColumnLen sets the length of the specified fields.
func (s *Schema) SetColumnLen(l int16, fields ...string) *Schema {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(fmt.Errorf("field does not exist: '%v'", v))
		}
		f.ColumnLength = l
	}
	return s
}

// SetColumnDefaultValue sets the default value for the specified fields.
func (s *Schema) SetColumnDefaultValue(defaultValue string, fields ...string) *Schema {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(fmt.Errorf("field does not exist: '%v'", v))
		}
		f.ColumnDefaultValue = defaultValue
	}
	return s
}

// SetAutoIncrementColumn sets the specified field as an auto-increment column.
func (s *Schema) SetAutoIncrementColumn(field string) *Schema {
	f := s.GetField(field)
	if f == nil {
		panic(fmt.Errorf("field does not exist: '%v'", field))
	}
	f.AutoIncrement = true

	return s
}

// SetSeparateTable configures table sharding for the schema.
func (s *Schema) SetSeparateTable(separateType EnumSeparateType) *Schema {
	s.separateTable = &SeparateTable{
		tableName:     s.TableName,
		SeparateType:  separateType,
		LastCheckTime: time.Now(),
	}
	return s
}

// SetColumnNull sets whether the specified fields can be null.
func (s *Schema) SetColumnNull(columnNull bool, fields ...string) *Schema {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(fmt.Errorf("field does not exist: '%v'", v))
		}
		f.ColumnNull = columnNull
	}
	return s
}
