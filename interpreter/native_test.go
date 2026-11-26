package interpreter

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/truora/minidyn/types"
)

var boolTrue = true

func TestNativeMatch(t *testing.T) {
	item := map[string]*types.Item{
		"a": {
			S: new("a"),
		},
		"n": {
			N: new("1"),
		},
		"b": {
			BOOL: &boolTrue,
		},
		"txt": {
			S: new("hello world"),
		},
	}

	input := MatchInput{
		TableName:      "test",
		Expression:     ":a = a",
		Item:           item,
		ExpressionType: ExpressionTypeConditional,
		Attributes: map[string]*types.Item{
			":a": {
				S: new("a"),
			},
		},
	}

	native := NewNativeInterpreter()

	_, err := native.Match(input)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Error("match without a defined expression should fail")
	}

	native.AddMatcher("test", ExpressionTypeConditional, ":a = a", func(m1, m2 map[string]*types.Item) bool {
		return true
	})

	matched, err := native.Match(input)
	if err != nil {
		t.Error("match with a defined expression should not fail")
	}

	if !matched {
		t.Error("input should match")
	}
}

func TestAddMatcher(t *testing.T) {
	native := NewNativeInterpreter()
	etypes := []ExpressionType{
		ExpressionTypeConditional,
		ExpressionTypeFilter,
		ExpressionTypeKey,
	}

	for _, etype := range etypes {
		native.AddMatcher("test", etype, ":a = a", func(m1, m2 map[string]*types.Item) bool {
			return true
		})
	}

	if len(native.keyExpressions) != 1 {
		t.Errorf("key expression should be 1 but got %d", len(native.keyExpressions))
	}

	if len(native.filterExpressions) != 1 {
		t.Errorf("filter expression should be 1 but got %d", len(native.keyExpressions))
	}

	if len(native.writeCondExpressions) != 1 {
		t.Errorf("write conditional expression should be 1 but got %d", len(native.keyExpressions))
	}

	for _, etype := range etypes {
		matcher, err := native.getMatcher("test", ":a = a", etype)
		if err != nil {
			t.Errorf("get %s should not fail but got %s error", etype, err)
		}

		if matcher == nil {
			t.Errorf("expression %s should be found", etype)
		}
	}
}

func TestNativeUpdate(t *testing.T) {
	item := map[string]*types.Item{
		"a": {
			S: new("a"),
		},
		"n": {
			N: new("1"),
		},
		"b": {
			BOOL: &boolTrue,
		},
		"txt": {
			S: new("hello world"),
		},
	}

	input := UpdateInput{
		TableName:  "test",
		Expression: "set a = :b",
		Item:       item,
		Attributes: map[string]*types.Item{
			":b": {
				S: new("foo"),
			},
		},
	}

	native := NewNativeInterpreter()

	err := native.Update(input)
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Error("update without a defined expression should fail")
	}

	native.AddUpdater("test", "set a = :b", func(m1, m2 map[string]*types.Item) {
		m1["a"] = m2[":b"]
	})

	err = native.Update(input)
	if err != nil {
		t.Error("match with a defined expression should not fail")
	}

	if types.StringValue(item["a"].S) != "foo" {
		t.Error("item should have been updated")
	}
}

func TestNativeConcurrentRegistration(t *testing.T) {
	ni := NewNativeInterpreter()
	kinds := []ExpressionType{ExpressionTypeKey, ExpressionTypeFilter, ExpressionTypeConditional}

	for _, kind := range kinds {
		ni.AddMatcher("t", kind, "ok", yesMatcher)
	}

	ni.AddUpdater("t", "SET a = :a", func(_, _ map[string]*types.Item) {})

	var wg sync.WaitGroup

	for w := range 3 {
		wg.Go(func() { registerMany(ni, kinds, w) })
	}

	for range 3 {
		wg.Go(func() { readMany(t, ni, kinds) })
	}

	wg.Wait()
}

func yesMatcher(_, _ map[string]*types.Item) bool { return true }

func registerMany(ni *Native, kinds []ExpressionType, worker int) {
	for i := range 200 {
		expr := fmt.Sprintf("expr-%d-%d", worker, i)

		for _, kind := range kinds {
			ni.AddMatcher("t", kind, expr, yesMatcher)
		}

		ni.AddUpdater("t", expr, func(_, _ map[string]*types.Item) {})
	}
}

func readMany(t *testing.T, ni *Native, kinds []ExpressionType) {
	for range 200 {
		for _, kind := range kinds {
			ok, err := ni.Match(MatchInput{TableName: "t", Expression: "ok", ExpressionType: kind})
			if err != nil || !ok {
				t.Errorf("Match(%q) = %v, %v", kind, ok, err)
			}
		}

		if err := ni.Update(UpdateInput{TableName: "t", Expression: "SET a = :a"}); err != nil {
			t.Errorf("Update: %v", err)
		}
	}
}

// A callback may register expressions on the same Native it was called from, so
// Match and Update must not hold the lock while running it.
func TestNativeCallbackCanRegister(t *testing.T) {
	ni := NewNativeInterpreter()

	ni.AddMatcher("t", ExpressionTypeFilter, "reg", func(_, _ map[string]*types.Item) bool {
		ni.AddMatcher("t", ExpressionTypeFilter, "late", func(_, _ map[string]*types.Item) bool { return true })
		ni.AddUpdater("t", "late", func(_, _ map[string]*types.Item) {})

		return true
	})
	ni.AddUpdater("t", "reg", func(_, _ map[string]*types.Item) {
		ni.AddMatcher("t", ExpressionTypeKey, "late", func(_, _ map[string]*types.Item) bool { return true })
		ni.AddUpdater("t", "late2", func(_, _ map[string]*types.Item) {})
	})

	finishes := func(t *testing.T, name string, fn func()) {
		t.Helper()

		done := make(chan struct{})

		go func() {
			defer close(done)

			fn()
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not return: the lock is held while the callback runs", name)
		}
	}

	finishes(t, "Match", func() {
		ok, err := ni.Match(MatchInput{TableName: "t", Expression: "reg", ExpressionType: ExpressionTypeFilter})
		if err != nil || !ok {
			t.Errorf("Match = %v, %v", ok, err)
		}
	})

	finishes(t, "Update", func() {
		if err := ni.Update(UpdateInput{TableName: "t", Expression: "reg"}); err != nil {
			t.Errorf("Update: %v", err)
		}
	})
}
