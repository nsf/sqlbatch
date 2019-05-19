package sqlbatch

import (
	"github.com/codemodus/kace"
	"github.com/lib/pq"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

type StructFieldGetterFunc func(structPtr unsafe.Pointer, ifacePtr *interface{})
type StructFieldWriterFunc func(structPtr unsafe.Pointer, b *strings.Builder)

type FieldInfo struct {
	Name       string
	QuotedName string
	PrimaryKey bool
	GetValue   StructFieldGetterFunc
	WriteValue StructFieldWriterFunc
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
	Fields []FieldInfo
}

var structInfoCache = map[reflect.Type]*StructInfo{}
var structInfoCacheLock sync.RWMutex

var CustomStructFieldGetterFuncResolver StructFieldGetterFuncResolver
var CustomStructFieldWriterFuncResolver StructFieldWriterFuncResolver

func GetStructInfo(t reflect.Type, customGetter StructFieldGetterFuncResolver, customWriter StructFieldWriterFuncResolver) *StructInfo {
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

	info := ScanStruct(t, 0, customGetter, customWriter)
	structInfoCache[t] = info
	return info
}

type tagInfo struct {
	name       string
	primaryKey bool
	ignore     bool
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
			case "-":
				out.ignore = true
			}
		}
	}
	return
}

func ScanStruct(t reflect.Type, offset uintptr, customGetter StructFieldGetterFuncResolver, customWriter StructFieldWriterFuncResolver) *StructInfo {
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

		if f.Anonymous {
			// embedded field
			for _, ef := range ScanStruct(f.Type, f.Offset, customGetter, customWriter).Fields {
				addFieldMaybe(ef)
			}
		} else {
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

			if ti.ignore {
				continue
			}

			field := FieldInfo{
				Name:       kace.Snake(f.Name),
				GetValue:   MakeStructFieldGetterFuncForField(f, offset, customGetter),
				WriteValue: MakeStructFieldWriterFuncForField(f, offset, customWriter),
				PrimaryKey: ti.primaryKey,
			}
			if ti.name != "" {
				field.Name = ti.name
			}
			field.QuotedName = pq.QuoteIdentifier(field.Name)
			addFieldMaybe(field)
		}
	}

	return &StructInfo{
		Name:       structName,
		QuotedName: pq.QuoteIdentifier(structName),
		Fields:     fields,
	}
}

type Batch struct {
	stmtBuilder strings.Builder
}

func New() *Batch {
	return &Batch{}
}

func (b *Batch) Insert(v interface{}) {
	structVal := reflect.ValueOf(v)
	t := structVal.Type()
	if t.Kind() != reflect.Ptr {
		panic("pointer to struct expected")
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		panic("pointer to struct expected")
	}

	ptr := unsafe.Pointer(structVal.Pointer())
	si := GetStructInfo(t, CustomStructFieldGetterFuncResolver, CustomStructFieldWriterFuncResolver)

	sb := &b.stmtBuilder
	if sb.Len() != 0 {
		sb.WriteString("; ")
	}
	sb.WriteString("INSERT INTO ")
	sb.WriteString(si.QuotedName)
	sb.WriteString(" (")
	for i, f := range si.Fields {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(f.QuotedName)
	}
	sb.WriteString(") VALUES (")
	for i, f := range si.Fields {
		if i != 0 {
			sb.WriteString(", ")
		}
		f.WriteValue(ptr, sb)
	}
	sb.WriteString(") RETURNING NOTHING")
}

func (b *Batch) Query() string {
	return b.stmtBuilder.String()
}
