package concurrent_hashmap

import (
	"fmt"
	"hash/fnv"
	"iter"
	"maps"
	"reflect"
	"sync"
)

type ConcurrentHashMap[K comparable, V any] struct {
	shards    []*shard[K, V]
	shardMask uint32
}

type shard[K comparable, V any] struct {
	sync.RWMutex
	items map[K]V
}

func NewConcurrentHashMap[K comparable, V any](shardCount int) *ConcurrentHashMap[K, V] {
	shards := make([]*shard[K, V], shardCount)
	for i := range shards {
		shards[i] = &shard[K, V]{
			items: make(map[K]V),
		}
	}
	return &ConcurrentHashMap[K, V]{
		shards:    shards,
		shardMask: uint32(shardCount) - 1,
	}
}

func (c *ConcurrentHashMap[K, V]) Get(key K) (V, bool) {
	shard := c.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	val, ok := shard.items[key]
	return val, ok
}

func (c *ConcurrentHashMap[K, V]) Set(key K, value V) {
	shard := c.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.items[key] = value
}

func (c *ConcurrentHashMap[K, V]) Delete(key K) {
	shard := c.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.items, key)
}

func (c *ConcurrentHashMap[K, V]) Range(f func(key K, value V) bool) {
	for _, shard := range c.shards {
		shard.RLock()
		for k, v := range shard.items {
			if !f(k, v) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
}

func (c *ConcurrentHashMap[K, V]) getShard(key K) *shard[K, V] {
	h := fnv.New32a()
	keyString := fmt.Sprintf("%v", reflect.ValueOf(key))
	h.Write([]byte(keyString))
	return c.shards[h.Sum32()&c.shardMask]
}

func (c *ConcurrentHashMap[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, shard := range c.shards {
			shard.RLock()
			for k, v := range shard.items {
				if !yield(k, v) {
					shard.RUnlock()
					return
				}
			}
			shard.RUnlock()
		}
	}
}

func (c *ConcurrentHashMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for _, shard := range c.shards {
			shard.RLock()
			for k := range shard.items {
				if !yield(k) {
					shard.RUnlock()
					return
				}
			}
			shard.RUnlock()
		}
	}
}

func (c *ConcurrentHashMap[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, shard := range c.shards {
			shard.RLock()
			for _, v := range shard.items {
				if !yield(v) {
					shard.RUnlock()
					return
				}
			}
			shard.RUnlock()
		}
	}
}

func (c *ConcurrentHashMap[K, V]) Collect() map[K]V {
	return maps.Collect(c.All())
}

func (c *ConcurrentHashMap[K, V]) Insert(seq iter.Seq2[K, V]) {
	seq(func(k K, v V) bool {
		c.Set(k, v)
		return true
	})
}

func (c *ConcurrentHashMap[K, V]) Clone() *ConcurrentHashMap[K, V] {
	newMap := NewConcurrentHashMap[K, V](len(c.shards))
	for i, shard := range c.shards {
		shard.RLock()
		newMap.shards[i].items = maps.Clone(shard.items)
		shard.RUnlock()
	}
	return newMap
}

func (c *ConcurrentHashMap[K, V]) EqualFunc(other *ConcurrentHashMap[K, V], eq func(V, V) bool) bool {
	if len(c.shards) != len(other.shards) {
		return false
	}
	for i, shard := range c.shards {
		shard.RLock()
		otherShard := other.shards[i]
		otherShard.RLock()
		equal := maps.EqualFunc(shard.items, otherShard.items, eq)
		otherShard.RUnlock()
		shard.RUnlock()
		if !equal {
			return false
		}
	}
	return true
}
