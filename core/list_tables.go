package core

import (
	"sort"

	"github.com/truora/minidyn/types"
)

// maxListTablesLimit is the largest Limit ListTables accepts.
const maxListTablesLimit = 100

// ListTableNames returns one ListTables page: the table names sorted by byte
// order that come after exclusiveStart, at most limit of them (all when limit
// is nil), and the last returned name when more tables remain.
func ListTableNames(tables map[string]*Table, exclusiveStart string, limit *int32) ([]string, *string, error) {
	if limit != nil && *limit < 1 {
		return nil, nil, types.NewError("ValidationException", "Limit must be greater than or equal to 1", nil)
	}

	if limit != nil && *limit > maxListTablesLimit {
		return nil, nil, types.NewError("ValidationException", "Limit in ListTables must be <= 100", nil)
	}

	names := make([]string, 0, len(tables))

	for name := range tables {
		if name > exclusiveStart {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	if limit == nil || len(names) <= int(*limit) {
		return names, nil, nil
	}

	names = names[:*limit]
	last := names[len(names)-1]

	return names, &last, nil
}
