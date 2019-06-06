package sqlbatch

import (
	"strings"
)

type tagInfo struct {
	name       string
	primaryKey bool
	ignore     bool
	group      string
	isCreated  bool
	isUpdated  bool
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
			case "created":
				out.isCreated = true
			case "updated":
				out.isUpdated = true
			}
		}
	}
	return
}
