package sqlbatch

import (
	"database/sql"
	"github.com/codemodus/kace"
	"github.com/lib/pq"
	"reflect"
	"sync"
	"time"
)

type StructInfo struct {
	// name of the struct is converted to snake case
	Name       string
	QuotedName string

	// Fields are flattened, which means embedded struct fields are there too.
	// All field names are converted to snake case and must be unique,
	// if field is repeated it's skipped.
	//
	// When scanning the struct, looks for "gorm" or "db" or "sql" tags.
	// Understands the following:
	//   `db:"column:foo"`             - rename the column
	//   `db:"primary_key"`            - assume column is a primary key
	//   `db:"column:foo,primary_key"` - both (comma separated)
	//   `db:"-"`                      - skip the field
	//   `db:"group:bar"`              - for embedded structs, assign it to a group
	//   `db:"created"`                - must be time.Time or pq.NullTime, value assigned on Insert()
	//   `db:"updated"`                - must be time.Time or pq.NullTime, value assigned on Update()
	Fields         []FieldInfo
	PrimaryKeys    []*FieldInfo
	NonPrimaryKeys []*FieldInfo
}

func (si *StructInfo) FindField(name string) *FieldInfo {
	for i := range si.Fields {
		f := &si.Fields[i]
		if f.Name == name {
			return f
		}
	}
	return nil
}

func filterPrimaryKeys(fields []FieldInfo, v bool) []*FieldInfo {
	var out []*FieldInfo
	for i := range fields {
		if fields[i].IsPrimaryKey() == v {
			out = append(out, &fields[i])
		}
	}
	return out
}

var typeInfoCache = map[reflect.Type]*FieldInterface{}
var typeInfoCacheLock sync.RWMutex

var structInfoCache = map[reflect.Type]*StructInfo{}
var structInfoCacheLock sync.RWMutex

type FieldInterfaceResolver func(t reflect.Type, offset uintptr) (FieldInterface, bool)

func resolveCustomFieldInterface(t reflect.Type, offset uintptr, custom FieldInterfaceResolver) (FieldInterface, bool) {
	if custom == nil {
		return FieldInterface{}, false
	} else {
		return custom(t, offset)
	}
}

func makeInterfaceForType(t reflect.Type, o uintptr, custom FieldInterfaceResolver) FieldInterface {
	if iface, ok := resolveCustomFieldInterface(t, o, custom); ok {
		return iface
	}
	switch t.Kind() {
	case reflect.Bool:
		return FieldInterface{
			Set:    makeBoolSetter(o),
			GetPtr: makeBoolPtrGetter(o),
			Write:  makeBoolWriter(o),
			Conv:   boolConverter,
		}
	case reflect.Int:
		return FieldInterface{
			Set:    makeIntSetter(o),
			GetPtr: makeIntPtrGetter(o),
			Write:  makeIntWriter(o),
			Conv:   intConverter,
		}
	case reflect.Int8:
		return FieldInterface{
			Set:    makeInt8Setter(o),
			GetPtr: makeInt8PtrGetter(o),
			Write:  makeInt8Writer(o),
			Conv:   int8Converter,
		}
	case reflect.Int16:
		return FieldInterface{
			Set:    makeInt16Setter(o),
			GetPtr: makeInt16PtrGetter(o),
			Write:  makeInt16Writer(o),
			Conv:   int16Converter,
		}
	case reflect.Int32:
		return FieldInterface{
			Set:    makeInt32Setter(o),
			GetPtr: makeInt32PtrGetter(o),
			Write:  makeInt32Writer(o),
			Conv:   int32Converter,
		}
	case reflect.Int64:
		return FieldInterface{
			Set:    makeInt64Setter(o),
			GetPtr: makeInt64PtrGetter(o),
			Write:  makeInt64Writer(o),
			Conv:   int64Converter,
		}
	case reflect.Uint:
		return FieldInterface{
			Set:    makeUintSetter(o),
			GetPtr: makeUintPtrGetter(o),
			Write:  makeUintWriter(o),
			Conv:   uintConverter,
		}
	case reflect.Uint8:
		return FieldInterface{
			Set:    makeUint8Setter(o),
			GetPtr: makeUint8PtrGetter(o),
			Write:  makeUint8Writer(o),
			Conv:   uint8Converter,
		}
	case reflect.Uint16:
		return FieldInterface{
			Set:    makeUint16Setter(o),
			GetPtr: makeUint16PtrGetter(o),
			Write:  makeUint16Writer(o),
			Conv:   uint16Converter,
		}
	case reflect.Uint32:
		return FieldInterface{
			Set:    makeUint32Setter(o),
			GetPtr: makeUint32PtrGetter(o),
			Write:  makeUint32Writer(o),
			Conv:   uint32Converter,
		}
	case reflect.Uint64:
		return FieldInterface{
			Set:    makeUint64Setter(o),
			GetPtr: makeUint64PtrGetter(o),
			Write:  makeUint64Writer(o),
			Conv:   uint64Converter,
		}
	case reflect.String:
		return FieldInterface{
			Set:    makeStringSetter(o),
			GetPtr: makeStringPtrGetter(o),
			Write:  makeStringWriter(o),
			Conv:   stringConverter,
		}
	case reflect.Float32:
		return FieldInterface{
			Set:    makeFloat32Setter(o),
			GetPtr: makeFloat32PtrGetter(o),
			Write:  makeFloat32Writer(o),
			Conv:   float32Converter,
		}
	case reflect.Float64:
		return FieldInterface{
			Set:    makeFloat64Setter(o),
			GetPtr: makeFloat64PtrGetter(o),
			Write:  makeFloat64Writer(o),
			Conv:   float64Converter,
		}
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8: // byte slice
			return FieldInterface{
				Set:    makeByteSliceSetter(o),
				GetPtr: makeByteSlicePtrGetter(o),
				Write:  makeByteSliceWriter(o),
				Conv:   byteSliceConverter,
			}
		case reflect.Int64: // array of numbers
			return FieldInterface{
				Set:    makeInt64SliceSetter(o),
				GetPtr: makeInt64SlicePtrGetter(o),
				Write:  makeInt64SliceWriter(o),
				Conv:   int64SliceConverter,
			}
		case reflect.String: // array of strings
			return FieldInterface{
				Set:    makeStringSliceSetter(o),
				GetPtr: makeStringSlicePtrGetter(o),
				Write:  makeStringSliceWriter(o),
				Conv:   stringSliceConverter,
			}
		}
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return FieldInterface{
				Set:    makeTimeSetter(o),
				GetPtr: makeTimePtrGetter(o),
				Write:  makeTimeWriter(o),
				Conv:   timeConverter,
			}
		} else if t == reflect.TypeOf(sql.NullBool{}) {
			return FieldInterface{
				Set:    makeNullBoolSetter(o),
				GetPtr: makeNullBoolPtrGetter(o),
				Write:  makeNullBoolWriter(o),
				Conv:   nullBoolConverter,
			}
		} else if t == reflect.TypeOf(sql.NullFloat64{}) {
			return FieldInterface{
				Set:    makeNullFloat64Setter(o),
				GetPtr: makeNullFloat64PtrGetter(o),
				Write:  makeNullFloat64Writer(o),
				Conv:   nullFloat64Converter,
			}
		} else if t == reflect.TypeOf(sql.NullInt64{}) {
			return FieldInterface{
				Set:    makeNullInt64Setter(o),
				GetPtr: makeNullInt64PtrGetter(o),
				Write:  makeNullInt64Writer(o),
				Conv:   nullInt64Converter,
			}
		} else if t == reflect.TypeOf(sql.NullString{}) {
			return FieldInterface{
				Set:    makeNullStringSetter(o),
				GetPtr: makeNullStringPtrGetter(o),
				Write:  makeNullStringWriter(o),
				Conv:   nullStringConverter,
			}
		} else if t == reflect.TypeOf(pq.NullTime{}) {
			return FieldInterface{
				Set:    makeNullTimeSetter(o),
				GetPtr: makeNullTimePtrGetter(o),
				Write:  makeNullTimeWriter(o),
				Conv:   nullTimeConverter,
			}
		}
	case reflect.Interface:
		if t == reflect.TypeOf((*interface{})(nil)).Elem() {
			return FieldInterface{
				GetPtr: makeInterfacePtrGetter(o),
				Write:  makeInterfaceWriter(o, custom),
				Conv:   makeInterfaceConverter(custom),
			}
		}
	}

	panic("unsupported field type: " + t.String())
}

func MakeFieldInterfaceForField(t reflect.StructField, offset uintptr, custom FieldInterfaceResolver) FieldInterface {
	return makeInterfaceForType(t.Type, t.Offset+offset, custom)
}

func GetTypeInfo(t reflect.Type, custom FieldInterfaceResolver) *FieldInterface {
	// quick path, let's try reading saved value
	typeInfoCacheLock.RLock()
	v, ok := typeInfoCache[t]
	if ok {
		typeInfoCacheLock.RUnlock()
		return v
	}
	typeInfoCacheLock.RUnlock()

	// slow path here, let's scan the struct
	typeInfoCacheLock.Lock()
	defer typeInfoCacheLock.Unlock()

	info := makeInterfaceForType(t, 0, custom)
	typeInfoCache[t] = &info
	return &info
}

func GetStructInfo(t reflect.Type, custom FieldInterfaceResolver) *StructInfo {
	// quick path, let's try reading saved value
	structInfoCacheLock.RLock()
	v, ok := structInfoCache[t]
	if ok {
		structInfoCacheLock.RUnlock()
		return v
	}
	structInfoCacheLock.RUnlock()

	// slow path here, let's scan the struct
	structInfoCacheLock.Lock()
	defer structInfoCacheLock.Unlock()

	info := ScanStruct(t, 0, custom)
	structInfoCache[t] = info
	return info
}

func assertTypeIsTime(t reflect.Type) {
	timeType := reflect.TypeOf(time.Time{})
	nullTimeType := reflect.TypeOf(pq.NullTime{})
	if t != timeType && t != nullTimeType {
		panic("time.Time or pq.NullTime expected")
	}
}

func isTypeNull(t reflect.Type) bool {
	switch {
	case t == reflect.TypeOf(sql.NullBool{}),
		t == reflect.TypeOf(sql.NullFloat64{}),
		t == reflect.TypeOf(sql.NullInt64{}),
		t == reflect.TypeOf(sql.NullString{}),
		t == reflect.TypeOf(pq.NullTime{}):
		return true
	default:
		return false
	}
}

type scanStructCtx struct {
	custom FieldInterfaceResolver
	offset uintptr
	group  string
}

func scanStructImpl(t reflect.Type, ctx *scanStructCtx) *StructInfo {
	if t.Kind() != reflect.Struct {
		panic("struct type expected")
	}

	structName := kace.Snake(t.Name())
	fields := []FieldInfo{}
	fieldsMap := map[string]struct{}{}

	addFieldMaybe := func(f FieldInfo) {
		if _, ok := fieldsMap[f.Name]; !ok {
			fieldsMap[f.Name] = struct{}{}
			fields = append(fields, f)
		}
	}

	for i, n := 0, t.NumField(); i < n; i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			// unexported field
			continue
		}

		var tagVal string
		var ok bool
		tagVal, ok = f.Tag.Lookup("db")
		if !ok {
			tagVal, ok = f.Tag.Lookup("sql")
			if !ok {
				tagVal, ok = f.Tag.Lookup("gorm")
			}
		}
		var ti tagInfo
		if ok {
			ti = parseTag(tagVal)
		}

		if f.Anonymous {
			// embedded field
			emCtx := &scanStructCtx{
				custom: ctx.custom,
				offset: ctx.offset + f.Offset,
				group:  ti.group,
			}
			for _, ef := range scanStructImpl(f.Type, emCtx).Fields {
				addFieldMaybe(ef)
			}
		} else {
			if ti.ignore {
				continue
			}

			if ti.isCreated || ti.isUpdated {
				assertTypeIsTime(f.Type)
			}

			flags := FieldInfoFlag(0)
			if ti.primaryKey {
				flags |= FieldInfoIsPrimaryKey
			}
			if ti.isCreated {
				flags |= FieldInfoIsCreated
			}
			if ti.isUpdated {
				flags |= FieldInfoIsUpdated
			}
			if isTypeNull(f.Type) {
				flags |= FieldInfoIsNull
			}

			field := FieldInfo{
				flags:     flags,
				Name:      kace.Snake(f.Name),
				Interface: MakeFieldInterfaceForField(f, ctx.offset, ctx.custom),
				Group:     ctx.group,
				Type:      f.Type,
			}
			if ti.name != "" {
				field.Name = ti.name
			}
			field.QuotedName = pq.QuoteIdentifier(field.Name)
			addFieldMaybe(field)
		}
	}

	return &StructInfo{
		Name:           structName,
		QuotedName:     pq.QuoteIdentifier(structName),
		Fields:         fields,
		PrimaryKeys:    filterPrimaryKeys(fields, true),
		NonPrimaryKeys: filterPrimaryKeys(fields, false),
	}
}

func ScanStruct(t reflect.Type, offset uintptr, custom FieldInterfaceResolver) *StructInfo {
	return scanStructImpl(t, &scanStructCtx{
		offset: offset,
		custom: custom,
	})
}
