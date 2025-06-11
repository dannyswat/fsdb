package fsdb

import (
	"reflect"
)

// StructToMap converts any struct to map[string]any using reflection.
func StructToMap(input any) map[string]any {
	result := make(map[string]any)
	v := reflect.ValueOf(input)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	if v.Kind() != reflect.Struct {
		return result
	}
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { // skip unexported fields
			continue
		}
		name := field.Name
		result[name] = v.Field(i).Interface()
	}
	return result
}

func MapToStruct[T any](m map[string]any) T {
	var result T
	v := reflect.ValueOf(&result).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { // skip unexported fields
			continue
		}
		name := field.Name
		if value, ok := m[name]; ok {
			fieldValue := v.Field(i)
			fieldType := field.Type

			// Handle pointer to struct
			if fieldType.Kind() == reflect.Ptr && fieldType.Elem().Kind() == reflect.Struct {
				if valueMap, isMap := value.(map[string]any); isMap {
					ptr := reflect.New(fieldType.Elem())
					ptr.Elem().Set(reflect.ValueOf(MapToStructHelper(valueMap, fieldType.Elem())))
					fieldValue.Set(ptr)
					continue
				}
			}
			// Handle struct
			if fieldType.Kind() == reflect.Struct {
				if valueMap, isMap := value.(map[string]any); isMap {
					fieldValue.Set(reflect.ValueOf(MapToStructHelper(valueMap, fieldType)))
					continue
				}
			}
			// Default: set value directly if assignable
			if reflect.TypeOf(value) != nil && reflect.TypeOf(value).AssignableTo(fieldType) {
				fieldValue.Set(reflect.ValueOf(value))
			}
		}
	}
	return result
}

// MapToStructHelper is a helper for MapToStruct to handle arbitrary struct types
func MapToStructHelper(m map[string]any, typ reflect.Type) any {
	result := reflect.New(typ).Elem()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := field.Name
		if value, ok := m[name]; ok {
			fieldValue := result.Field(i)
			fieldType := field.Type
			if fieldType.Kind() == reflect.Ptr && fieldType.Elem().Kind() == reflect.Struct {
				if valueMap, isMap := value.(map[string]any); isMap {
					ptr := reflect.New(fieldType.Elem())
					ptr.Elem().Set(reflect.ValueOf(MapToStructHelper(valueMap, fieldType.Elem())))
					fieldValue.Set(ptr)
					continue
				}
			}
			if fieldType.Kind() == reflect.Struct {
				if valueMap, isMap := value.(map[string]any); isMap {
					fieldValue.Set(reflect.ValueOf(MapToStructHelper(valueMap, fieldType)))
					continue
				}
			}
			if reflect.TypeOf(value) != nil && reflect.TypeOf(value).AssignableTo(fieldType) {
				fieldValue.Set(reflect.ValueOf(value))
			}
		}
	}
	return result.Interface()
}
