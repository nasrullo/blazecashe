package blazecache

import (
	"testing"
)

func TestRoundRobinSelection(t *testing.T) {
	c, err := New("A", "B", "C")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c = c.WithStrategy(RoundRobin)

	got := []string{
		c.selectServer("k"),
		c.selectServer("k"),
		c.selectServer("k"),
		c.selectServer("k"),
		c.selectServer("k"),
		c.selectServer("k"),
	}
	want := []string{"A", "B", "C", "A", "B", "C"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("round robin mismatch at %d: got %s want %s", i, got[i], want[i])
		}
	}
}

func TestConsistentHashingDeterminism(t *testing.T) {
	c, _ := New("A", "B")
	c = c.WithStrategy(ConsistentHashing)

	s1 := c.selectServer("alpha")
	s2 := c.selectServer("alpha")
	if s1 != s2 {
		t.Fatalf("consistent hashing not deterministic: %s vs %s", s1, s2)
	}
}
