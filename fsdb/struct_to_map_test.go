package fsdb

import (
	"reflect"
	"testing"
)

type testStruct struct {
	ID    int
	Name  string
	Value float64
	priv  string // unexported, should be skipped
}

type nestedStruct struct {
	Field1 string
	Field2 int
}

type structWithPtr struct {
	Ptr *int
}

func TestStructToMap_Basic(t *testing.T) {
	s := testStruct{ID: 1, Name: "foo", Value: 3.14, priv: "secret"}
	m := StructToMap(s)

	expected := map[string]any{
		"ID":    1,
		"Name":  "foo",
		"Value": 3.14,
	}

	if !reflect.DeepEqual(m, expected) {
		t.Errorf("StructToMap() = %v, want %v", m, expected)
	}

	// Unexported field should not be present
	if _, ok := m["priv"]; ok {
		t.Error("Unexported field 'priv' should not be present in map")
	}
}

func TestStructToMap_Pointer(t *testing.T) {
	s := &testStruct{ID: 2, Name: "bar", Value: 2.71}
	m := StructToMap(s)

	expected := map[string]any{
		"ID":    2,
		"Name":  "bar",
		"Value": 2.71,
	}

	if !reflect.DeepEqual(m, expected) {
		t.Errorf("StructToMap(pointer) = %v, want %v", m, expected)
	}
}

func TestStructToMap_NonStruct(t *testing.T) {
	m := StructToMap(123)
	if len(m) != 0 {
		t.Errorf("StructToMap(non-struct) = %v, want empty map", m)
	}
}

func TestStructToMap_NestedStruct(t *testing.T) {
	n := nestedStruct{Field1: "abc", Field2: 42}
	m := StructToMap(n)

	expected := map[string]any{
		"Field1": "abc",
		"Field2": 42,
	}

	if !reflect.DeepEqual(m, expected) {
		t.Errorf("StructToMap(nested) = %v, want %v", m, expected)
	}
}

func TestStructToMap_StructWithPtrField(t *testing.T) {
	val := 99
	s := structWithPtr{Ptr: &val}
	m := StructToMap(s)

	if m["Ptr"] != &val {
		t.Errorf("StructToMap(ptr field) = %v, want Ptr to be pointer to %d", m, val)
	}
}

func TestMapToStruct_Basic(t *testing.T) {
	m := map[string]any{
		"ID":    3,
		"Name":  "baz",
		"Value": 1.23,
	}

	s := MapToStruct[testStruct](m)

	expected := testStruct{ID: 3, Name: "baz", Value: 1.23}

	if !reflect.DeepEqual(s, expected) {
		t.Errorf("MapToStruct() = %v, want %v", s, expected)
	}
}
func TestMapToStruct_UnexportedField(t *testing.T) {
	m := map[string]any{
		"ID":    4,
		"Name":  "qux",
		"Value": 4.56,
		"priv":  "should not be set",
	}

	s := MapToStruct[testStruct](m)

	expected := testStruct{ID: 4, Name: "qux", Value: 4.56}

	if !reflect.DeepEqual(s, expected) {
		t.Errorf("MapToStruct() = %v, want %v", s, expected)
	}

	if s.priv != "" {
		t.Error("Unexported field 'priv' should not be set")
	}
}
func TestMapToStruct_MissingFields(t *testing.T) {
	m := map[string]any{
		"ID":   5,
		"Name": "missing value",
	}

	s := MapToStruct[testStruct](m)

	expected := testStruct{ID: 5, Name: "missing value", Value: 0.0} // Value should default to zero

	if !reflect.DeepEqual(s, expected) {
		t.Errorf("MapToStruct(missing fields) = %v, want %v", s, expected)
	}
}
func TestMapToStruct_ExtraFields(t *testing.T) {
	m := map[string]any{
		"ID":    6,
		"Name":  "extra fields",
		"Value": 7.89,
		"Extra": "should be ignored",
	}

	s := MapToStruct[testStruct](m)

	expected := testStruct{ID: 6, Name: "extra fields", Value: 7.89}

	if !reflect.DeepEqual(s, expected) {
		t.Errorf("MapToStruct(extra fields) = %v, want %v", s, expected)
	}
}
func TestMapToStruct_NonStruct(t *testing.T) {
	m := map[string]any{
		"ID":    7,
		"Name":  "not a struct",
		"Value": 8.90,
	}

	s := MapToStruct[testStruct](m)

	expected := testStruct{ID: 7, Name: "not a struct", Value: 8.90}

	if !reflect.DeepEqual(s, expected) {
		t.Errorf("MapToStruct(non-struct) = %v, want %v", s, expected)
	}
}
func TestMapToStruct_NestedStruct(t *testing.T) {
	m := map[string]any{
		"Field1": "nested",
		"Field2": 100,
	}

	n := MapToStruct[nestedStruct](m)

	expected := nestedStruct{Field1: "nested", Field2: 100}

	if !reflect.DeepEqual(n, expected) {
		t.Errorf("MapToStruct(nested) = %v, want %v", n, expected)
	}
}
func TestMapToStruct_StructWithPtrField(t *testing.T) {
	val := 42
	m := map[string]any{
		"Ptr": &val,
	}

	s := MapToStruct[structWithPtr](m)

	if s.Ptr == nil || *s.Ptr != val {
		t.Errorf("MapToStruct(ptr field) = %v, want Ptr to be pointer to %d", s, val)
	}
}
func TestMapToStruct_PtrFieldNil(t *testing.T) {
	m := map[string]any{
		"Ptr": nil,
	}

	s := MapToStruct[structWithPtr](m)

	if s.Ptr != nil {
		t.Error("MapToStruct(ptr field nil) should result in Ptr being nil")
	}
}
func TestMapToStruct_PtrFieldMissing(t *testing.T) {
	m := map[string]any{
		"ID":   8,
		"Name": "ptr missing",
	}

	s := MapToStruct[structWithPtr](m)

	if s.Ptr != nil {
		t.Error("MapToStruct(ptr field missing) should result in Ptr being nil")
	}
}
func TestMapToStruct_PtrFieldWithValue(t *testing.T) {
	val := 123
	m := map[string]any{
		"Ptr": &val,
	}

	s := MapToStruct[structWithPtr](m)

	if s.Ptr == nil || *s.Ptr != val {
		t.Errorf("MapToStruct(ptr field with value) = %v, want Ptr to be pointer to %d", s, val)
	}
}
