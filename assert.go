package sqlbatch

import (
	"reflect"
)

func assertSliceOfStructs(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Slice {
		panic("slice of structs expected")
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		panic("slice of structs expected")
	}
	return t
}

func assertPointerToStruct(t reflect.Type) reflect.Type {
	if t.Kind() != reflect.Ptr {
		panic("pointer to struct expected")
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		panic("pointer to struct expected")
	}
	return t
}

func assertPointerToStructOrSliceOfStructs(t reflect.Type) (reflect.Type, bool) {
	isSlice := false
	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
		switch t.Kind() {
		case reflect.Struct:
			// do nothing
		default:
			panic("pointer to struct or slice of structs expected")
		}
	case reflect.Slice:
		isSlice = true
		t = t.Elem()
		if t.Kind() != reflect.Struct {
			panic("pointer to struct or slice of structs expected")
		}
	}
	return t, isSlice
}

func assertPointerOrPointerToSlice(t reflect.Type) (reflect.Type, bool) {
	if t.Kind() != reflect.Ptr {
		panic("pointer to value or pointer to slice of values")
	}
	t = t.Elem()
	return t, t.Kind() == reflect.Slice
}

func assertPointerToStructOrPointerToSliceOfStructs(t reflect.Type) (reflect.Type, bool) {
	isSlice := false
	if t.Kind() != reflect.Ptr {
		panic("pointer to struct or pointer to slice of structs expected")
	}
	t = t.Elem()
	switch t.Kind() {
	case reflect.Slice:
		isSlice = true
		t = t.Elem()
		if t.Kind() != reflect.Struct {
			panic("pointer to struct or pointer to slice of structs expected")
		}
	case reflect.Struct:
		// do nothing
	default:
		panic("pointer to struct or pointer to slice of structs expected")
	}
	return t, isSlice
}

func assertHasPrimaryKeys(si *StructInfo) {
	if len(si.PrimaryKeys) == 0 {
		panic("struct has no primary keys defined")
	}
}
