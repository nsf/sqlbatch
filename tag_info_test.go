package sqlbatch

import (
	"fmt"
	"testing"
)

func TestTagParse(t *testing.T) {
	cases := []struct {
		v        string
		expected tagInfo
	}{
		{"", tagInfo{}},
		{"primary_key", tagInfo{primaryKey: true}},
		{"foo", tagInfo{}},
		{"default", tagInfo{isDefault: true}},
		{"-", tagInfo{ignore: true}},
		{"column:foo", tagInfo{name: "foo"}},
		{"group:bar", tagInfo{group: "bar"}},
		{"column:foo,primary_key", tagInfo{name: "foo", primaryKey: true}},
		{"primary_key,column:foo", tagInfo{name: "foo", primaryKey: true}},
		{"primary_key,column:foo,-", tagInfo{name: "foo", primaryKey: true, ignore: true}},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			v := parseTag(c.v)
			if v != c.expected {
				t.Fail()
			}
		})
	}
}
