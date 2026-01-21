package snapshotadvisor

import "testing"

func TestSortAndTruncate(t *testing.T) {
	cands := []Candidate{
		{Source: NodeRef{Host: "b", Port: 1}, Score: 50, reachable: true, shouldHaveShards: true},
		{Source: NodeRef{Host: "a", Port: 1}, Score: 50, reachable: true, shouldHaveShards: true},
		{Source: NodeRef{Host: "c", Port: 1}, Score: 60, reachable: false, shouldHaveShards: true},
	}
	out := sortAndTruncate(cands, 2)
	if len(out) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(out))
	}
	if out[0].Source.Host != "c" {
		t.Fatalf("expected highest score first")
	}
	if out[1].Source.Host != "a" {
		t.Fatalf("expected tie-breaker by host")
	}
}
