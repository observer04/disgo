package store

import (
	"sort"
)

type SortedSetNode struct {
	Member string
	Score  float64
}

type SortedSet struct {
	dict map[string]float64
	list []SortedSetNode
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		dict: make(map[string]float64),
		list: make([]SortedSetNode, 0),
	}
}

// Add adds a member with a score. Returns 1 if new element was added, 0 if updated.
func (ss *SortedSet) Add(score float64, member string) int {
	// Check if exists
	existingScore, exists := ss.dict[member]
	if exists {
		if existingScore == score {
			return 0
		}
		// Remove old node to update
		ss.removeNode(existingScore, member)
	}

	ss.dict[member] = score
	ss.insertNode(score, member)

	if exists {
		return 0
	}
	return 1
}

func (ss *SortedSet) insertNode(score float64, member string) {
	node := SortedSetNode{Member: member, Score: score}
	// Find insertion index
	idx := sort.Search(len(ss.list), func(i int) bool {
		n := ss.list[i]
		if n.Score == score {
			return n.Member >= member
		}
		return n.Score > score
	})

	// Insert at idx
	ss.list = append(ss.list, SortedSetNode{})
	copy(ss.list[idx+1:], ss.list[idx:])
	ss.list[idx] = node
}

func (ss *SortedSet) removeNode(score float64, member string) {
	idx := sort.Search(len(ss.list), func(i int) bool {
		n := ss.list[i]
		if n.Score == score {
			return n.Member >= member
		}
		return n.Score > score
	})

	// Verify found
	if idx < len(ss.list) && ss.list[idx].Member == member && ss.list[idx].Score == score {
		ss.list = append(ss.list[:idx], ss.list[idx+1:]...)
	}
}

// Range returns elements in the range [start, stop] (inclusive), 0-based indices.
// Indices can be negative.
func (ss *SortedSet) Range(start, stop int) []SortedSetNode {
	l := len(ss.list)
	if l == 0 {
		return nil
	}

	if start < 0 {
		start = l + start
	}
	if stop < 0 {
		stop = l + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= l {
		stop = l - 1
	}

	if start > stop {
		return nil
	}

	return ss.list[start : stop+1]
}

func (ss *SortedSet) Len() int {
	return len(ss.list)
}

// ZScore returns the score of the member and whether it exists
func (ss *SortedSet) ZScore(member string) (float64, bool) {
	s, ok := ss.dict[member]
	return s, ok
}

// Remove removes a member from the sorted set. Returns 1 if removed, 0 if not found.
func (ss *SortedSet) Remove(member string) int {
	score, ok := ss.dict[member]
	if !ok {
		return 0
	}
	delete(ss.dict, member)
	ss.removeNode(score, member)
	return 1
}

// ZAdd adds members to the sorted set stored at key.
func (k *Kv) ZAdd(key string, members ...SortedSetNode) int {
	k.mu.Lock()
	defer k.mu.Unlock()

	ss, ok := k.sortedSets[key]
	if !ok {
		ss = NewSortedSet()
		k.sortedSets[key] = ss
	}

	addedCount := 0
	for _, member := range members {
		addedCount += ss.Add(member.Score, member.Member)
	}
	return addedCount
}

// ZRange returns elements in the range [start, stop]
func (k *Kv) ZRange(key string, start, stop int) []SortedSetNode {
	k.mu.Lock()
	defer k.mu.Unlock()

	ss, ok := k.sortedSets[key]
	if !ok {
		return nil
	}
	return ss.Range(start, stop)
}

// ZCard returns the number of elements in the sorted set
func (k *Kv) ZCard(key string) int {
	k.mu.Lock()
	defer k.mu.Unlock()

	ss, ok := k.sortedSets[key]
	if !ok {
		return 0
	}
	return ss.Len()
}

// ZRem removes members from the sorted set stored at key
func (k *Kv) ZRem(key string, members ...string) int {
	k.mu.Lock()
	defer k.mu.Unlock()

	ss, ok := k.sortedSets[key]
	if !ok {
		return 0
	}

	removedCount := 0
	for _, member := range members {
		removedCount += ss.Remove(member)
	}

	if ss.Len() == 0 {
		delete(k.sortedSets, key)
	}

	return removedCount
}
