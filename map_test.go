package concurrent_hashmap

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
)

func TestConcurrentHashMapWithIntKeys(t *testing.T) {
	m := NewConcurrentHashMap[int, string](16)

	// Test Set and Get
	m.Set(1, "one")
	m.Set(2, "two")
	m.Set(3, "three")

	if v, ok := m.Get(1); !ok || v != "one" {
		t.Errorf("Expected 'one', got '%v'", v)
	}

	// Test Delete
	m.Delete(2)
	if _, ok := m.Get(2); ok {
		t.Error("Expected key 2 to be deleted")
	}

	// Test Range
	count := 0
	m.Range(func(key int, value string) bool {
		count++
		return true
	})
	if count != 2 {
		t.Errorf("Expected 2 items, got %d", count)
	}

	// Test Collect
	collected := m.Collect()
	if len(collected) != 2 {
		t.Errorf("Expected 2 items in collected map, got %d", len(collected))
	}

	// Test Clone
	clone := m.Clone()
	if !reflect.DeepEqual(m.Collect(), clone.Collect()) {
		t.Error("Cloned map is not equal to original")
	}

	// Test EqualFunc
	if !m.EqualFunc(clone, func(a, b string) bool { return a == b }) {
		t.Error("EqualFunc failed for equal maps")
	}
}

func TestConcurrentHashMapWithStringKeys(t *testing.T) {
	m := NewConcurrentHashMap[string, int](16)

	// Test Set and Get
	m.Set("one", 1)
	m.Set("two", 2)
	m.Set("three", 3)

	if v, ok := m.Get("two"); !ok || v != 2 {
		t.Errorf("Expected 2, got %v", v)
	}

	// Test Keys
	keys := []string{}
	m.Keys()(func(k string) bool {
		keys = append(keys, k)
		return true
	})
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// Test Values
	values := []int{}
	m.Values()(func(v int) bool {
		values = append(values, v)
		return true
	})
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	// Test Insert
	m.Insert(func(yield func(string, int) bool) {
		yield("four", 4)
		yield("five", 5)
	})
	if v, ok := m.Get("four"); !ok || v != 4 {
		t.Errorf("Expected 4, got %v", v)
	}
}

type complexKey struct {
	id   int
	name string
}

func TestConcurrentHashMapWithComplexKeys(t *testing.T) {
	m := NewConcurrentHashMap[complexKey, float64](16)

	// Test Set and Get
	m.Set(complexKey{1, "one"}, 1.1)
	m.Set(complexKey{2, "two"}, 2.2)
	m.Set(complexKey{3, "three"}, 3.3)

	if v, ok := m.Get(complexKey{2, "two"}); !ok || v != 2.2 {
		t.Errorf("Expected 2.2, got %v", v)
	}

	// Test All
	count := 0
	m.All()(func(k complexKey, v float64) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("Expected 3 items, got %d", count)
	}
}

func TestConcurrentOperations(t *testing.T) {
	m := NewConcurrentHashMap[int, int](32)
	var wg sync.WaitGroup
	numOps := 1000

	// Concurrent Set operations
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i, i*i)
		}(i)
	}

	// Concurrent Get operations
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(i)
		}(i)
	}

	// Concurrent Delete operations
	for i := 0; i < numOps/2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Delete(i * 2)
		}(i)
	}

	wg.Wait()

	// Verify results
	count := 0
	m.Range(func(key, value int) bool {
		count++
		if key%2 == 0 && key < numOps {
			t.Errorf("Even key %d should have been deleted", key)
		}
		return true
	})

	expectedCount := numOps - numOps/2
	if count != expectedCount {
		t.Errorf("Expected %d items, got %d", expectedCount, count)
	}
}

func TestEdgeCases(t *testing.T) {
	m := NewConcurrentHashMap[string, interface{}](1) // Single shard

	// Test with empty string key
	m.Set("", "empty")
	if v, ok := m.Get(""); !ok || v != "empty" {
		t.Errorf("Failed to get empty string key")
	}

	// Test with nil value
	m.Set("nil", nil)
	if v, ok := m.Get("nil"); !ok || v != nil {
		t.Errorf("Failed to get nil value")
	}

	// Test deleting non-existent key
	m.Delete("non-existent")

	// Test getting non-existent key
	if _, ok := m.Get("non-existent"); ok {
		t.Errorf("Get should return false for non-existent key")
	}

	// Test Range with early termination
	count := 0
	m.Range(func(key string, value interface{}) bool {
		count++
		return false // Stop after first item
	})
	if count != 1 {
		t.Errorf("Range should have stopped after first item")
	}
}

func TestLargeDataSet(t *testing.T) {
	m := NewConcurrentHashMap[int, string](64)
	numItems := 100000

	// Insert large number of items
	for i := 0; i < numItems; i++ {
		m.Set(i, strconv.Itoa(i))
	}

	// Verify all items
	for i := 0; i < numItems; i++ {
		if v, ok := m.Get(i); !ok || v != strconv.Itoa(i) {
			t.Errorf("Failed to get item %d", i)
		}
	}

	// Test Clone with large dataset
	clone := m.Clone()
	if !reflect.DeepEqual(m.Collect(), clone.Collect()) {
		t.Error("Cloned large map is not equal to original")
	}
}

func TestConcurrentHashMapWithFloatKeys(t *testing.T) {
	m := NewConcurrentHashMap[float64, string](16)

	// Test Set and Get with float keys
	m.Set(1.1, "one point one")
	m.Set(2.2, "two point two")
	m.Set(-3.3, "minus three point three")

	if v, ok := m.Get(2.2); !ok || v != "two point two" {
		t.Errorf("Expected 'two point two', got '%v'", v)
	}

	// Test with very small float difference
	m.Set(0.1, "zero point one")
	m.Set(0.1000000000000001, "zero point one (with small difference)")

	if v, ok := m.Get(0.1); !ok || v != "zero point one" {
		t.Errorf("Expected 'zero point one', got '%v'", v)
	}

	if v, ok := m.Get(0.1000000000000001); !ok || v != "zero point one (with small difference)" {
		t.Errorf("Expected 'zero point one (with small difference)', got '%v'", v)
	}
}

func TestConcurrentHashMapWithBoolKeys(t *testing.T) {
	m := NewConcurrentHashMap[bool, int](2)

	// Test Set and Get with boolean keys
	m.Set(true, 1)
	m.Set(false, 0)

	if v, ok := m.Get(true); !ok || v != 1 {
		t.Errorf("Expected 1, got %v", v)
	}

	if v, ok := m.Get(false); !ok || v != 0 {
		t.Errorf("Expected 0, got %v", v)
	}

	// Test overwriting values
	m.Set(true, 100)
	if v, ok := m.Get(true); !ok || v != 100 {
		t.Errorf("Expected 100, got %v", v)
	}
}

func TestConcurrentHashMapWithStructKeys(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	m := NewConcurrentHashMap[Person, string](16)

	// Test Set and Get with struct keys
	m.Set(Person{"Alice", 30}, "Software Engineer")
	m.Set(Person{"Bob", 35}, "Product Manager")
	m.Set(Person{"Charlie", 25}, "Designer")

	if v, ok := m.Get(Person{"Bob", 35}); !ok || v != "Product Manager" {
		t.Errorf("Expected 'Product Manager', got '%v'", v)
	}

	// Test with struct key having same field values
	m.Set(Person{"Alice", 30}, "Senior Software Engineer")
	if v, ok := m.Get(Person{"Alice", 30}); !ok || v != "Senior Software Engineer" {
		t.Errorf("Expected 'Senior Software Engineer', got '%v'", v)
	}
}

func BenchmarkConcurrentHashMap(b *testing.B) {
	m := NewConcurrentHashMap[int, int](32)

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Set(rand.Int(), i)
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(rand.Int())
		}
	})

	b.Run("Delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Delete(rand.Int())
		}
	})

	b.Run("Range", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m.Range(func(key, value int) bool {
				return true
			})
		}
	})
}

func ExampleConcurrentHashMap() {
	m := NewConcurrentHashMap[string, int](16)

	m.Set("one", 1)
	m.Set("two", 2)
	m.Set("three", 3)

	fmt.Println(m.Get("two"))
	fmt.Println(m.Get("four"))

	m.Delete("one")

	var keys []string
	m.Range(func(key string, value int) bool {
		keys = append(keys, key)
		return true
	})

	// Sort the keys to ensure consistent output
	sort.Strings(keys)

	for _, key := range keys {
		value, _ := m.Get(key)
		fmt.Printf("%s: %d\n", key, value)
	}

	// Output:
	// 2 true
	// 0 false
	// three: 3
	// two: 2
}
