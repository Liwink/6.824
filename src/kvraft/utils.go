package raftkv

type CmdHeap []*Cmd

func (h CmdHeap) Len() int {
	return len(h)
}

func (h CmdHeap) Less(i, j int) bool {
	return h[i].Index < h[j].Index
}

func (h CmdHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *CmdHeap) Push(x interface{})  {
	*h = append(*h, x.(*Cmd))
}

func (h *CmdHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

