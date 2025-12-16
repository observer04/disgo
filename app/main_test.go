package main

import (
	"strconv"
	"sync"
	"testing"
)

func TestLPushBasic(t *testing.T) {
	kv := NewKv()
	n := kv.LPush("mylist", "one", "two", "three")
	if n != 3 {
		t.Fatalf("expected length 3, got %d", n)
	}
	got, err := kv.LRange("mylist", 0, -1)
	if err != nil {
		t.Fatalf("LRange error: %v", err)
	}
	want := []string{"one", "two", "three"}
	if len(got) != len(want) {
		t.Fatalf("expected list length %d, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at index %d expected %q got %q", i, want[i], got[i])
		}
	}
}

func TestLPushPrependToExisting(t *testing.T) {
	kv := NewKv()
	kv.RPush("letters", "x", "y")
	n := kv.LPush("letters", "a", "b")
	if n != 4 {
		t.Fatalf("expected length 4, got %d", n)
	}
	got, err := kv.LRange("letters", 0, -1)
	if err != nil {
		t.Fatalf("LRange error: %v", err)
	}
	want := []string{"a", "b", "x", "y"}
	if len(got) != len(want) {
		t.Fatalf("expected list length %d, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at index %d expected %q got %q", i, want[i], got[i])
		}
	}
}

func TestLPushConcurrent(t *testing.T) {
	kv := NewKv()
	const goroutines = 10
	const perGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				kv.LPush("concurrent", "v"+strconv.Itoa(id)+"-"+strconv.Itoa(j))
			}
		}(i)
	}
	wg.Wait()
	got, err := kv.LRange("concurrent", 0, -1)
	if err != nil {
		t.Fatalf("LRange error: %v", err)
	}
	expected := goroutines * perGoroutine
	if len(got) != expected {
		t.Fatalf("expected length %d, got %d", expected, len(got))
	}
	// spot-check some elements are present (not enforcing strict order due to concurrency)
	foundAny := false
	for _, v := range got {
		if len(v) > 0 {
			foundAny = true
			break
		}
	}
	if !foundAny {
		t.Fatalf("expected non-empty elements in list")
	}
}
