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

func filterPrimaryKeys(fields []FieldInfo, v bool) []*FieldInfo {
	var out []*FieldInfo
	for i := range fields {
		if fields[i].IsPrimaryKey() == v {
			out = append(out, &fields[i])
		}
	}
	return out
}

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

func MakeFieldInterfaceForField(t reflect.StructField, offset uintptr, custom FieldInterfaceResolver) FieldInterface {
	o := t.Offset + offset
	switch t.Type.Kind() {
	case reflect.Bool:
		return FieldInterface{
			Set:   makeBoolSetter(o),
			Get:   makeBoolGetter(o),
			Write: makeBoolWriter(o),
		}
	case reflect.Int:
		return FieldInterface{
			Set:   makeIntSetter(o),
			Get:   makeIntGetter(o),
			Write: makeIntWriter(o),
		}
	case reflect.Int8:
		return FieldInterface{
			Set:   makeInt8Setter(o),
			Get:   makeInt8Getter(o),
			Write: makeInt8Writer(o),
		}
	case reflect.Int16:
		return FieldInterface{
			Set:   makeInt16Setter(o),
			Get:   makeInt16Getter(o),
			Write: makeInt16Writer(o),
		}
	case reflect.Int32:
		return FieldInterface{
			Set:   makeInt32Setter(o),
			Get:   makeInt32Getter(o),
			Write: makeInt32Writer(o),
		}
	case reflect.Int64:
		return FieldInterface{
			Set:   makeInt64Setter(o),
			Get:   makeInt64Getter(o),
			Write: makeInt64Writer(o),
		}
	case reflect.Uint:
		return FieldInterface{
			Set:   makeUintSetter(o),
			Get:   makeUintGetter(o),
			Write: makeUintWriter(o),
		}
	case reflect.Uint8:
		return FieldInterface{
			Set:   makeUint8Setter(o),
			Get:   makeUint8Getter(o),
			Write: makeUint8Writer(o),
		}
	case reflect.Uint16:
		return FieldInterface{
			Set:   makeUint16Setter(o),
			Get:   makeUint16Getter(o),
			Write: makeUint16Writer(o),
		}
	case reflect.Uint32:
		return FieldInterface{
			Set:   makeUint32Setter(o),
			Get:   makeUint32Getter(o),
			Write: makeUint32Writer(o),
		}
	case reflect.Uint64:
		return FieldInterface{
			Set:   makeUint64Setter(o),
			Get:   makeUint64Getter(o),
			Write: makeUint64Writer(o),
		}
	case reflect.String:
		return FieldInterface{
			Set:   makeStringSetter(o),
			Get:   makeStringGetter(o),
			Write: makeStringWriter(o),
		}
	case reflect.Float32:
		return FieldInterface{
			Set:   makeFloat32Setter(o),
			Get:   makeFloat32Getter(o),
			Write: makeFloat32Writer(o),
		}
	case reflect.Float64:
		return FieldInterface{
			Set:   makeFloat64Setter(o),
			Get:   makeFloat64Getter(o),
			Write: makeFloat64Writer(o),
		}
	case reflect.Slice:
		if t.Type.Elem().Kind() == reflect.Uint8 { // byte slice
			return FieldInterface{
				Set:   makeByteSliceSetter(o),
				Get:   makeByteSliceGetter(o),
				Write: makeByteSliceWriter(o),
			}
		}
	case reflect.Struct:
		if iface, ok := resolveCustomFieldInterface(t.Type, o, custom); ok {
			return iface
		} else if t.Type == reflect.TypeOf(time.Time{}) {
			return FieldInterface{
				Set:   makeTimeSetter(o),
				Get:   makeTimeGetter(o),
				Write: makeTimeWriter(o),
			}
		} else if t.Type == reflect.TypeOf(sql.NullBool{}) {
			return FieldInterface{
				Set:   makeNullBoolSetter(o),
				Get:   makeNullBoolGetter(o),
				Write: makeNullBoolWriter(o),
			}
		} else if t.Type == reflect.TypeOf(sql.NullFloat64{}) {
			return FieldInterface{
				Set:   makeNullFloat64Setter(o),
				Get:   makeNullFloat64Getter(o),
				Write: makeNullFloat64Writer(o),
			}
		} else if t.Type == reflect.TypeOf(sql.NullInt64{}) {
			return FieldInterface{
				Set:   makeNullInt64Setter(o),
				Get:   makeNullInt64Getter(o),
				Write: makeNullInt64Writer(o),
			}
		} else if t.Type == reflect.TypeOf(sql.NullString{}) {
			return FieldInterface{
				Set:   makeNullStringSetter(o),
				Get:   makeNullStringGetter(o),
				Write: makeNullStringWriter(o),
			}
		} else if t.Type == reflect.TypeOf(pq.NullTime{}) {
			return FieldInterface{
				Set:   makeNullTimeSetter(o),
				Get:   makeNullTimeGetter(o),
				Write: makeNullTimeWriter(o),
			}
		}
	}
	panic("unsupported field type: " + t.Type.String())
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
