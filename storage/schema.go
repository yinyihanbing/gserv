package storage

import (
	"fmt"
	"reflect"
	"errors"
	"time"
	"go/ast"

	"github.com/yinyihanbing/gutils/logs"
)

type SchemaManager struct {
	schemas map[reflect.Type]*Schema
}

// 结构体信息
type Schema struct {
	Type          reflect.Type
	TableName     string
	Fields        []*Field
	IndexKeys     [][]string
	separateTable *SeparateTable // 分表存储配置(不分表为nil)
}

type Field struct {
	Name               string
	Type               reflect.Type
	ColumnName         string         // 列名
	ColumnType         EnumColumnType // 列类型
	ColumnLength       int16          // 长度
	ColumnNull         bool           // 可为空
	ColumnDefaultValue string         // 默认值
	PrimaryKey         bool           // 主键
	AutoIncrement      bool           // 自增
}

func newSchemaManager() *SchemaManager {
	s := &SchemaManager{}
	s.schemas = map[reflect.Type]*Schema{}
	return s
}

// 注册结构体概要信息, pks:主键属性名...
func (s *SchemaManager) Register(p interface{}, pks ...string) (*Schema) {
	reflectType := GetStructType(p)

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
				panic(fmt.Errorf("register schema error! struct: %v, error: %v", reflectType.Name(), err))
			}
			schema.Fields = append(schema.Fields, &Field{Name: fieldStruct.Name, Type: fieldStruct.Type, ColumnName: cName, ColumnType: cType, ColumnLength: cLength, ColumnDefaultValue: cDefaultValue, PrimaryKey: false})
		}
	}
	// 设置主键
	schema.setTablePrimaryKeys(pks...)

	s.schemas[schema.Type] = schema
	logs.Debug("schema register success: %v", schema.Type)

	return schema
}

func (s *SchemaManager) GetAllSchema() map[reflect.Type]*Schema {
	return s.schemas
}

// 获取结构体概述, p:类型引用
func (s *SchemaManager) GetSchema(p interface{}) (*Schema, error) {
	reflectType := GetStructType(p)
	if v, ok := s.schemas[reflectType]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("schema not exists: %v", reflectType)
}

// 获取分表名称
func (s *Schema) GetSeparateTableName() (isSeparate bool, separateTableName string) {
	if s.separateTable == nil {
		return false, ""
	}
	return s.separateTable.IsNowSeparate()
}

// 查找字段
func (s *Schema) GetField(field string) *Field {
	for _, f := range s.Fields {
		if f.Name == field {
			return f
		}
	}
	return nil
}

// 设置表主键(主键自动增加索引)
func (s *Schema) setTablePrimaryKeys(fields ...string) {
	if len(fields) == 0 {
		return
	}
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(errors.New(fmt.Sprintf("not exists field '%v'", v)))
		}
		f.PrimaryKey = true
	}
	s.AddTableIdx(fields...)
}

// 添加表索引
func (s *Schema) AddTableIdx(fields ...string) (*Schema) {
	if len(fields) == 0 {
		return s
	}
	clo := make([]string, 0)
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(errors.New(fmt.Sprintf("not exists field '%v'", v)))
		}
		clo = append(clo, f.ColumnName)
	}
	s.IndexKeys = append(s.IndexKeys, clo)

	return s
}

// 设置列时间类型, ct:数据库存储列类型, fields:属性名
func (s *Schema) SetDateTimeColumnType(fields ...string) (*Schema) {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(errors.New(fmt.Sprintf("not exists field '%v'", v)))
		}
		f.ColumnType = ColumnTypeDatetime
		f.ColumnLength = 0
	}

	return s
}

// 设置列长度, l:数据库存储列长度, fields:属性名
func (s *Schema) SetColumnLen(l int16, fields ...string) (*Schema) {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(errors.New(fmt.Sprintf("not exists field '%v'", v)))
		}
		f.ColumnLength = l
	}
	return s
}

// 设置列默认值, defaultValue:默认值, fields:属性名
func (s *Schema) SetColumnDefaultValue(defaultValue string, fields ...string) (*Schema) {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(errors.New(fmt.Sprintf("not exists field '%v'", v)))
		}
		f.ColumnDefaultValue = defaultValue
	}
	return s
}

// 设置自增列, fields:属性名, 注: 只有在新建表才有效
func (s *Schema) SetAutoIncrementColumn(field string) (*Schema) {
	f := s.GetField(field)
	if f == nil {
		panic(errors.New(fmt.Sprintf("not exists field '%v'", field)))
	}
	f.AutoIncrement = true

	return s
}

// 设置分表存储, 服务器启动时会记录一个时间点, 当插入语句创建时间和记录点的间隔满足配置时,会重命名表名为分表表名, 重新创建新表插入
func (s *Schema) SetSeparateTable(separateType EnumSeparateType) (*Schema) {
	s.separateTable = &SeparateTable{
		tableName:     s.TableName,
		SeparateType:  separateType,
		LastCheckTime: time.Now(),
	}
	return s
}

// 设置列是否可为空(默认不可为空), columnNull:是否可为空, fields:属性名
func (s *Schema) SetColumnNull(columnNull bool, fields ...string) (*Schema) {
	for _, v := range fields {
		f := s.GetField(v)
		if f == nil {
			panic(errors.New(fmt.Sprintf("not exists field '%v'", v)))
		}
		f.ColumnNull = columnNull
	}
	return s
}
