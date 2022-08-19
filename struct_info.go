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
	//   `db:"default"`                - override field value to DEFAULT on INSERT
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
			Set:    MakeSetter[bool](o),
			GetPtr: MakePtrGetter[bool](o),
			Write:  makeBoolWriter(o),
			Conv:   boolConverter,
		}
	case reflect.Int:
		return FieldInterface{
			Set:    MakeSetter[int](o),
			GetPtr: MakePtrGetter[int](o),
			Write:  makeGenericIntWriter[int](o),
			Conv:   genericIntConverter[int],
		}
	case reflect.Int8:
		return FieldInterface{
			Set:    MakeSetter[int8](o),
			GetPtr: MakePtrGetter[int8](o),
			Write:  makeGenericIntWriter[int8](o),
			Conv:   genericIntConverter[int8],
		}
	case reflect.Int16:
		return FieldInterface{
			Set:    MakeSetter[int16](o),
			GetPtr: MakePtrGetter[int16](o),
			Write:  makeGenericIntWriter[int16](o),
			Conv:   genericIntConverter[int16],
		}
	case reflect.Int32:
		return FieldInterface{
			Set:    MakeSetter[int32](o),
			GetPtr: MakePtrGetter[int32](o),
			Write:  makeGenericIntWriter[int32](o),
			Conv:   genericIntConverter[int32],
		}
	case reflect.Int64:
		return FieldInterface{
			Set:    MakeSetter[int64](o),
			GetPtr: MakePtrGetter[int64](o),
			Write:  makeGenericIntWriter[int64](o),
			Conv:   genericIntConverter[int64],
		}
	case reflect.Uint:
		return FieldInterface{
			Set:    MakeSetter[uint](o),
			GetPtr: MakePtrGetter[uint](o),
			Write:  makeGenericUintWriter[uint](o),
			Conv:   genericUintConverter[uint],
		}
	case reflect.Uint8:
		return FieldInterface{
			Set:    MakeSetter[uint8](o),
			GetPtr: MakePtrGetter[uint8](o),
			Write:  makeGenericUintWriter[uint8](o),
			Conv:   genericUintConverter[uint8],
		}
	case reflect.Uint16:
		return FieldInterface{
			Set:    MakeSetter[uint16](o),
			GetPtr: MakePtrGetter[uint16](o),
			Write:  makeGenericUintWriter[uint16](o),
			Conv:   genericUintConverter[uint16],
		}
	case reflect.Uint32:
		return FieldInterface{
			Set:    MakeSetter[uint32](o),
			GetPtr: MakePtrGetter[uint32](o),
			Write:  makeGenericUintWriter[uint32](o),
			Conv:   genericUintConverter[uint32],
		}
	case reflect.Uint64:
		return FieldInterface{
			Set:    MakeSetter[uint64](o),
			GetPtr: MakePtrGetter[uint64](o),
			Write:  makeGenericUintWriter[uint64](o),
			Conv:   genericUintConverter[uint64],
		}
	case reflect.String:
		return FieldInterface{
			Set:    MakeSetter[string](o),
			GetPtr: MakePtrGetter[string](o),
			Write:  makeStringWriter(o),
			Conv:   stringConverter,
		}
	case reflect.Float32:
		return FieldInterface{
			Set:    MakeSetter[float32](o),
			GetPtr: MakePtrGetter[float32](o),
			Write:  makeFloat32Writer(o),
			Conv:   float32Converter,
		}
	case reflect.Float64:
		return FieldInterface{
			Set:    MakeSetter[float64](o),
			GetPtr: MakePtrGetter[float64](o),
			Write:  makeFloat64Writer(o),
			Conv:   float64Converter,
		}
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8: // byte slice
			return FieldInterface{
				Set:    MakeSetter[[]byte](o),
				GetPtr: MakePtrGetter[[]byte](o),
				Write:  makeByteSliceWriter(o),
				Conv:   byteSliceConverter,
			}
		case reflect.Int64: // array of numbers
			return FieldInterface{
				Set:    MakeSetter[[]int64](o),
				GetPtr: MakePtrGetter[[]int64](o),
				Write:  makeInt64SliceWriter(o),
				Conv:   int64SliceConverter,
			}
		case reflect.String: // array of strings
			return FieldInterface{
				Set:    MakeSetter[[]string](o),
				GetPtr: MakePtrGetter[[]string](o),
				Write:  makeStringSliceWriter(o),
				Conv:   stringSliceConverter,
			}
		}
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return FieldInterface{
				Set:    MakeSetter[time.Time](o),
				GetPtr: MakePtrGetter[time.Time](o),
				Write:  makeTimeWriter(o),
				Conv:   timeConverter,
			}
		} else if t == reflect.TypeOf(sql.NullBool{}) {
			return FieldInterface{
				Set:    MakeSetter[sql.NullBool](o),
				GetPtr: MakePtrGetter[sql.NullBool](o),
				Write:  makeNullBoolWriter(o),
				Conv:   nullBoolConverter,
			}
		} else if t == reflect.TypeOf(sql.NullFloat64{}) {
			return FieldInterface{
				Set:    MakeSetter[sql.NullFloat64](o),
				GetPtr: MakePtrGetter[sql.NullFloat64](o),
				Write:  makeNullFloat64Writer(o),
				Conv:   nullFloat64Converter,
			}
		} else if t == reflect.TypeOf(sql.NullInt64{}) {
			return FieldInterface{
				Set:    MakeSetter[sql.NullInt64](o),
				GetPtr: MakePtrGetter[sql.NullInt64](o),
				Write:  makeNullInt64Writer(o),
				Conv:   nullInt64Converter,
			}
		} else if t == reflect.TypeOf(sql.NullString{}) {
			return FieldInterface{
				Set:    MakeSetter[sql.NullString](o),
				GetPtr: MakePtrGetter[sql.NullString](o),
				Write:  makeNullStringWriter(o),
				Conv:   nullStringConverter,
			}
		} else if t == reflect.TypeOf(pq.NullTime{}) {
			return FieldInterface{
				Set:    MakeSetter[pq.NullTime](o),
				GetPtr: MakePtrGetter[pq.NullTime](o),
				Write:  makeNullTimeWriter(o),
				Conv:   nullTimeConverter,
			}
		}
	case reflect.Interface:
		if t == reflect.TypeOf((*any)(nil)).Elem() {
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
			if ti.isDefault {
				flags |= FieldInfoIsDefault
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
