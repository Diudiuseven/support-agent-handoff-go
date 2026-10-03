package agent

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/Diudiuseven/support-agent-handoff-go/domain"
)

func TestReserveCapacityAndIdempotency(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Profile{ID: "A", Capacity: 2, Queues: nil}); err != nil {
		t.Fatal(err)
	}
	r.SetOnline("A", true)
	ok, err := r.TryReserve("A", "s1")
	if err != nil || !ok {
		t.Fatalf("reserve1 %v %v", ok, err)
	}
	ok, _ = r.TryReserve("A", "s2")
	if !ok {
		t.Fatal("reserve2")
	}
	// Idempotent re-reservation does not consume capacity.
	ok, _ = r.TryReserve("A", "s1")
	if !ok {
		t.Fatal("re-reserve should be idempotent")
	}
	s, _ := r.Get("A")
	if s.Load != 2 {
		t.Fatalf("load=%d", s.Load)
	}
	// Third distinct session rejected at capacity.
	if _, err := r.TryReserve("A", "s3"); !errors.Is(err, domain.ErrAgentAtCapacity) {
		t.Fatalf("want capacity err, got %v", err)
	}
	r.Release("A", "s1")
	if s, _ := r.Get("A"); s.Load != 1 {
		t.Fatalf("load after release = %d", s.Load)
	}
}

func TestReserveOffline(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(Profile{ID: "A", Capacity: 5})
	if _, err := r.TryReserve("A", "s"); !errors.Is(err, domain.ErrAgentOffline) {
		t.Fatalf("got %v", err)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

func TestConcurrentReserveSingleWinner(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(Profile{ID: "A", Capacity: 1})
	r.SetOnline("A", true)
	var wg sync.WaitGroup
	wins := make(chan bool, 100)
	start := make(chan struct{})
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ok, _ := r.TryReserve("A", domain.SessionID("s-"+itoa(i)))
			wins <- ok
		}(i)
	}
	close(start)
	wg.Wait()
	close(wins)
	n := 0
	for w := range wins {
		if w {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("winners=%d", n)
	}
}
