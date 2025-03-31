package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yinyihanbing/gutils"
	"google.golang.org/protobuf/proto"
)

// ChangleName converts a camelCase string to a snake_case string in lowercase.
func ChangleName(str string) string {
	var buf bytes.Buffer
	for i := range len(str) {
		if i != 0 && str[i] >= 'A' && str[i] <= 'Z' {
			buf.WriteString("_")
		}
		buf.WriteString(strings.ToLower(string(str[i])))
	}
	return buf.String()
}

// GetValueContainer initializes a container for field values based on the schema.
func GetValueContainer(schema *Schema) []any {
	values := make([]any, len(schema.Fields))
	for fieldIndex, field := range schema.Fields {
		// handle datetime type
		if field.ColumnType == ColumnTypeDatetime {
			var strText string
			values[fieldIndex] = &strText
		} else {
			// handle other types
			switch field.Type.Kind() {
			case reflect.Array, reflect.Slice, reflect.Map, reflect.Struct, reflect.Ptr:
				var strText string
				values[fieldIndex] = &strText
			default:
				values[fieldIndex] = reflect.New(field.Type).Interface()
			}
		}
	}
	return values
}

// TransformRowData maps field value containers to a struct instance.
func TransformRowData(schema *Schema, vContainer []any, p any) (err error) {
	rv := reflect.ValueOf(p)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	for j, field := range schema.Fields {
		value := reflect.ValueOf(vContainer[j]).Elem()
		// handle datetime type
		if field.ColumnType == ColumnTypeDatetime {
			t, err := gutils.ParseTime(value.String())
			if err != nil {
				return fmt.Errorf("parse time error: string=%v, err=%v", value.String(), err)
			}
			rv.FieldByName(field.Name).Set(reflect.ValueOf(t.Unix()))
			continue
		} else if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Uint8 {
			// handle byte slices
			rv.FieldByName(field.Name).SetBytes([]byte(value.String()))
		} else {
			// handle other types
			switch field.Type.Kind() {
			case reflect.Ptr, reflect.Map, reflect.Struct, reflect.Array, reflect.Slice:
				jsonStr := value.String()
				if jsonStr == "" {
					switch field.Type.Kind() {
					case reflect.Ptr, reflect.Map:
						jsonStr = "{}"
					case reflect.Struct, reflect.Slice, reflect.Array:
						jsonStr = "[]"
					default:
						jsonStr = ""
					}
				}
				m := reflect.New(field.Type).Interface()
				err = json.Unmarshal([]byte(jsonStr), m)
				if err != nil {
					err = fmt.Errorf("json unmarshal error: table=%v, column=%v, src=%v, err=%v", schema.TableName, field.ColumnName, value.String(), err)
					break
				}
				rv.FieldByName(field.Name).Set(reflect.ValueOf(m).Elem())
			default:
				rv.FieldByName(field.Name).Set(value)
			}
		}
	}
	return err
}

// ParseColumnValue converts a field value to its storage representation.
func ParseColumnValue(field *Field, v any) (any, error) {
	k := field.Type.Kind()

	// handle datetime type
	if field.ColumnType == ColumnTypeDatetime {
		if k == reflect.Int64 {
			tm := time.Unix(v.(int64), 0)
			data := tm.Format("2006-01-02 15:04:05")
			return data, nil
		} else if k == reflect.String {
			return v, nil
		} else {
			return nil, fmt.Errorf("parse datetime column error: value[%v %v]", field.Type, v)
		}
	}

	// handle byte slices with Chinese characters
	if k == reflect.Slice && field.Type.Elem().Kind() == reflect.Uint8 {
		return string(escapeBackslash(v.([]byte))), nil
	}

	// handle other types
	switch k {
	case reflect.Bool:
		if v.(bool) {
			return 1, nil
		}
		return 0, nil
	case reflect.String:
		return string(escapeBackslash([]byte(v.(string)))), nil
	case reflect.Map, reflect.Struct, reflect.Array, reflect.Slice, reflect.Ptr:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("parse column value error: type[%v], value[%v], err[%v]", field.Type, v, err)
		}
		return string(escapeBackslash(data)), nil
	}
	return v, nil
}

// GetStructType retrieves the underlying struct type from slice, map, or pointer types.
func GetStructType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Map || t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// TransferRedisValToVal converts a Redis value to the corresponding Go type.
func TransferRedisValToVal(redisValue any, t reflect.Type) (result any, err error) {
	st := GetStructType(t)
	valueKind := st.Kind()

	switch valueKind {
	case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16, reflect.Int, reflect.Int32:
		v, err := redis.Int(redisValue, err)
		if err != nil {
			return nil, err
		}
		switch valueKind {
		case reflect.Int8:
			result = int8(v)
		case reflect.Uint8:
			result = uint8(v)
		case reflect.Int16:
			result = int16(v)
		case reflect.Uint16:
			result = uint16(v)
		case reflect.Int:
			result = int(v)
		case reflect.Int32:
			result = int32(v)
		}
	case reflect.String:
		result, err = redis.String(redisValue, err)
	case reflect.Bool:
		result, err = redis.Bool(redisValue, err)
	case reflect.Float64:
		result, err = redis.Float64(redisValue, err)
	case reflect.Uint64:
		result, err = redis.Uint64(redisValue, err)
	case reflect.Int64:
		result, err = redis.Int64(redisValue, err)
	case reflect.Uint, reflect.Uint32:
		v, err := redis.Int64(redisValue, err)
		if err != nil {
			return nil, err
		}
		switch valueKind {
		case reflect.Uint:
			result = uint(v)
		case reflect.Uint32:
			result = uint32(v)
		}
	case reflect.Struct:
		result = reflect.New(st).Interface()
		err = proto.Unmarshal(redisValue.([]byte), result.(proto.Message))
	default:
		return nil, fmt.Errorf("get redis value error: type=%v, value=%v", valueKind, redisValue)
	}

	return
}

// TransferValToRedisVal converts a Go value to its Redis storage representation.
func TransferValToRedisVal(v any) (redisVal any, err error) {
	if _, ok := v.(proto.Message); ok {
		redisVal, err = proto.Marshal(v.(proto.Message))
		return
	}
	return v, nil
}
