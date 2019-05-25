package sqlbatch

import (
	"database/sql"
	"github.com/codemodus/kace"
	"github.com/lib/pq"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"
)

type FieldInterface struct {
	Get   func(structPtr unsafe.Pointer, ifacePtr *interface{})
	Write func(structPtr unsafe.Pointer, b *strings.Builder)
}

type FieldInfo struct {
	Name       string
	QuotedName string
	PrimaryKey bool
	Interface  FieldInterface
	Group      string
}

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
	Fields         []FieldInfo
	PrimaryKeys    []*FieldInfo
	NonPrimaryKeys []*FieldInfo
}

func filterPrimaryKeys(fields []FieldInfo, v bool) []*FieldInfo {
	var out []*FieldInfo
	for i := range fields {
		if fields[i].PrimaryKey == v {
			out = append(out, &fields[i])
		}
	}
	return out
}

var structInfoCache = map[reflect.Type]*StructInfo{}
var structInfoCacheLock sync.RWMutex

type FieldInterfaceResolver func(t reflect.Type, offset uintptr) (FieldInterface, bool)

var CustomFieldInterfaceResolver FieldInterfaceResolver

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
		return FieldInterface{Get: makeBoolGetter(o), Write: makeBoolWriter(o)}
	case reflect.Int:
		return FieldInterface{Get: makeIntGetter(o), Write: makeIntWriter(o)}
	case reflect.Int8:
		return FieldInterface{Get: makeInt8Getter(o), Write: makeInt8Writer(o)}
	case reflect.Int16:
		return FieldInterface{Get: makeInt16Getter(o), Write: makeInt16Writer(o)}
	case reflect.Int32:
		return FieldInterface{Get: makeInt32Getter(o), Write: makeInt32Writer(o)}
	case reflect.Int64:
		return FieldInterface{Get: makeInt64Getter(o), Write: makeInt64Writer(o)}
	case reflect.Uint:
		return FieldInterface{Get: makeUintGetter(o), Write: makeUintWriter(o)}
	case reflect.Uint8:
		return FieldInterface{Get: makeUint8Getter(o), Write: makeUint8Writer(o)}
	case reflect.Uint16:
		return FieldInterface{Get: makeUint16Getter(o), Write: makeUint16Writer(o)}
	case reflect.Uint32:
		return FieldInterface{Get: makeUint32Getter(o), Write: makeUint32Writer(o)}
	case reflect.Uint64:
		return FieldInterface{Get: makeUint64Getter(o), Write: makeUint64Writer(o)}
	case reflect.String:
		return FieldInterface{Get: makeStringGetter(o), Write: makeStringWriter(o)}
	case reflect.Float32:
		return FieldInterface{Get: makeFloat32Getter(o), Write: makeFloat32Writer(o)}
	case reflect.Float64:
		return FieldInterface{Get: makeFloat64Getter(o), Write: makeFloat64Writer(o)}
	case reflect.Slice:
		if t.Type.Elem().Kind() == reflect.Uint8 { // byte slice
			return FieldInterface{Get: makeByteSliceGetter(o), Write: makeByteSliceWriter(o)}
		}
	case reflect.Struct:
		if iface, ok := resolveCustomFieldInterface(t.Type, o, custom); ok {
			return iface
		} else if t.Type == reflect.TypeOf(time.Time{}) {
			return FieldInterface{Get: makeTimeGetter(o), Write: makeTimeWriter(o)}
		} else if t.Type == reflect.TypeOf(sql.NullBool{}) {
			return FieldInterface{Get: makeNullBoolGetter(o), Write: makeNullBoolWriter(o)}
		} else if t.Type == reflect.TypeOf(sql.NullFloat64{}) {
			return FieldInterface{Get: makeNullFloat64Getter(o), Write: makeNullFloat64Writer(o)}
		} else if t.Type == reflect.TypeOf(sql.NullInt64{}) {
			return FieldInterface{Get: makeNullInt64Getter(o), Write: makeNullInt64Writer(o)}
		} else if t.Type == reflect.TypeOf(sql.NullString{}) {
			return FieldInterface{Get: makeNullStringGetter(o), Write: makeNullStringWriter(o)}
		} else if t.Type == reflect.TypeOf(pq.NullTime{}) {
			return FieldInterface{Get: makeNullTimeGetter(o), Write: makeNullTimeWriter(o)}
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

type tagInfo struct {
	name       string
	primaryKey bool
	ignore     bool
	group      string
}

func parseTag(t string) (out tagInfo) {
	vals := strings.Split(t, ",")
	for _, v := range vals {
		kv := strings.Split(v, ":")
		if len(kv) > 0 {
			switch kv[0] {
			case "primary_key":
				out.primaryKey = true
			case "column":
				if len(kv) > 1 {
					out.name = kv[1]
				}
			case "group":
				if len(kv) > 1 {
					out.group = kv[1]
				}
			case "-":
				out.ignore = true
			}
		}
	}
	return
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

			field := FieldInfo{
				Name:       kace.Snake(f.Name),
				Interface:  MakeFieldInterfaceForField(f, ctx.offset, ctx.custom),
				PrimaryKey: ti.primaryKey,
				Group:      ctx.group,
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

type Batch struct {
	stmtBuilder strings.Builder
}

func New() *Batch {
	return &Batch{}
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

func assertHasPrimaryKeys(si *StructInfo) {
	if len(si.PrimaryKeys) == 0 {
		panic("struct has no primary keys defined")
	}
}

func writePrimaryKeysWhereCondition(si *StructInfo, ptr unsafe.Pointer, sb *strings.Builder) {
	pkWriter := newListWriter(sb)
	for _, f := range si.PrimaryKeys {
		b := pkWriter.Next()
		b.WriteString(f.QuotedName)
		b.WriteString(" = ")
		f.Interface.Write(ptr, b)
	}
	sb.WriteString(" RETURNING NOTHING")
}

func (b *Batch) beginNextStmt() *strings.Builder {
	sb := &b.stmtBuilder
	if sb.Len() != 0 {
		sb.WriteString("; ")
	}
	return sb
}

func (b *Batch) Insert(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)

	sb := b.beginNextStmt()
	sb.WriteString("INSERT INTO ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" (")
	fieldNamesWriter := newListWriter(sb)
	for _, f := range si.Fields {
		fieldNamesWriter.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	fieldValuesWriter := newListWriter(sb)
	for _, f := range si.Fields {
		f.Interface.Write(ptr, fieldValuesWriter.Next())
	}
	sb.WriteString(") RETURNING NOTHING")
	return b
}

func (b *Batch) Update(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("UPDATE ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" SET ")
	valsWriter := newListWriter(sb)
	for _, f := range si.NonPrimaryKeys {
		b := valsWriter.Next()
		b.WriteString(f.QuotedName)
		b.WriteString(" = ")
		f.Interface.Write(ptr, b)
	}
	sb.WriteString(" WHERE ")
	writePrimaryKeysWhereCondition(si, ptr, sb)
	return b
}

func (b *Batch) Delete(v interface{}) *Batch {
	structVal := reflect.ValueOf(v)
	t := assertPointerToStruct(structVal.Type())

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomFieldInterfaceResolver)
	assertHasPrimaryKeys(si)

	sb := b.beginNextStmt()
	sb.WriteString("DELETE FROM ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" WHERE ")
	writePrimaryKeysWhereCondition(si, ptr, sb)
	return b
}

func (b *Batch) Query() string {
	return b.stmtBuilder.String()
}
