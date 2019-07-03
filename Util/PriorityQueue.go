package Util

import "strings"

type Node struct {
	Value string
	index int
}

type PriorityQueue []*Node

//pq is immutable for the first 3 functions
func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return strings.Compare(pq[i].Value, pq[j].Value) == 1
	// return pq[i].value > pq[j].value
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Modifying pq, so need a pointer
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	node := x.(*Node)
	node.index = n
	*pq = append(*pq, node)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	node := old[n-1]
	// need to set the index back
	node.index = -1
	*pq = old[0 : n-1]
	return node
}
