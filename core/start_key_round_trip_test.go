package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// Paging with Limit must visit what an unpaged Query returns, each item once, in the same
// order: every LastEvaluatedKey minidyn hands out has to be accepted as an ExclusiveStartKey
// (#171), including the one a page that ends exactly on an inclusive bound returns (#153).

type roundTripCond struct {
	name string
	expr func(attr string) string
	// operands names the placeholders the expression uses, among :lo, :hi and :pre.
	operands    []string
	stringsOnly bool
}

var roundTripConds = []roundTripCond{
	{"=", func(a string) string { return a + " = :lo" }, []string{":lo"}, false},
	{"<", func(a string) string { return a + " < :hi" }, []string{":hi"}, false},
	{"<=", func(a string) string { return a + " <= :hi" }, []string{":hi"}, false},
	{">", func(a string) string { return a + " > :lo" }, []string{":lo"}, false},
	{">=", func(a string) string { return a + " >= :lo" }, []string{":lo"}, false},
	{"BETWEEN", func(a string) string { return a + " BETWEEN :lo AND :hi" }, []string{":lo", ":hi"}, false},
	{"begins_with", func(a string) string { return "begins_with(" + a + ", :pre)" }, []string{":pre"}, true},
}

type roundTripTarget struct {
	name, index, hashAttr, hash, attr, typ, lo, hi string
}

// roundTripInputs is every Query the round trip pages: each target, condition, filter and
// direction, keyed by name.
func roundTripInputs(sortKeyType, lo, hi string) map[string]QueryInput {
	targets := []roundTripTarget{
		{"table", "", "pk", "p", "sk", sortKeyType, lo, hi},
		{"LSI", "by-l", "pk", "p", "l", "S", "b", "c"},
		{"GSI", "by-g", "g", "g", "gr", "S", "b", "c"},
	}

	inputs := map[string]QueryInput{}

	for _, target := range targets {
		for _, cond := range roundTripConds {
			if cond.stringsOnly && target.typ != "S" {
				continue
			}

			for _, filtered := range []bool{false, true} {
				for _, forward := range []bool{true, false} {
					name := fmt.Sprintf("%s/%s/filter=%v/forward=%v", target.name, cond.name, filtered, forward)
					inputs[name] = roundTripInput(target, cond, filtered, forward)
				}
			}
		}
	}

	return inputs
}

func roundTripInput(target roundTripTarget, cond roundTripCond, filtered, forward bool) QueryInput {
	all := map[string]*types.Item{
		":lo":  startKeyValue(target.typ, target.lo),
		":hi":  startKeyValue(target.typ, target.hi),
		":pre": skS("b"),
	}
	values := map[string]*types.Item{":h": skS(target.hash)}

	for _, op := range cond.operands {
		values[op] = all[op]
	}

	in := QueryInput{
		Index:                     target.index,
		KeyConditionExpression:    target.hashAttr + " = :h AND " + cond.expr(target.attr),
		ScanIndexForward:          forward,
		ExpressionAttributeValues: values,
	}

	if filtered {
		// Drops the item whose l (and gr) is "b", through an attribute that is no key of the
		// target: a Query cannot filter on its own keys.
		attr := "l"
		if target.attr == "l" {
			attr = "gr"
		}

		in.FilterExpression = attr + " <> :skip"
		values[":skip"] = skS("b")
	}

	return in
}

func TestSearchData_pagingVisitsTheUnpagedResult(t *testing.T) {
	// Every item holds one of these in its sort key and in its index keys, in different
	// orders, so that the walk of each index differs from the table's.
	idxKeys := []string{"c", "bb", "a", "b", "d"}

	for _, typ := range []string{"S", "N"} {
		sks, lo, hi := []string{"a", "b", "bb", "c", "d"}, "b", "c"
		if typ == "N" {
			sks, lo, hi = []string{"1", "2", "3", "4", "5"}, "2", "4"
		}

		table := startKeyTestTable(t, typ, sks, idxKeys)

		for name, in := range roundTripInputs(typ, lo, hi) {
			t.Run(typ+"/"+name, func(t *testing.T) {
				want := startKeyLabels(t, table, in, 0)

				for _, limit := range []int64{1, 2} {
					require.Equal(t, want, startKeyLabels(t, table, in, limit), "Limit %d", limit)
				}
			})
		}
	}
}

// startKeyLabels reads every page of in with limit (all at once when it is 0), feeding each
// LastEvaluatedKey back, and returns the sort keys of the items read.
func startKeyLabels(t *testing.T, table *Table, in QueryInput, limit int64) []string {
	t.Helper()

	in.Limit = limit
	labels := []string{}

	for page := 0; page < 50; page++ {
		res, err := table.SearchData(in)
		require.NoError(t, err, "page %d", page)

		for _, it := range res.Items {
			labels = append(labels, startKeyLabel(it["sk"]))
		}

		if len(res.LastEvaluatedKey) == 0 {
			return labels
		}

		in.ExclusiveStartKey = res.LastEvaluatedKey
	}

	require.FailNow(t, "paging did not finish")

	return nil
}
