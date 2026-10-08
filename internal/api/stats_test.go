package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Diudiuseven/support-agent-handoff-go/internal/service"
)

func TestStatsEndpoint(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var st service.Stats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.PendingByAgent == nil || st.ActiveByAgent == nil || st.CapacityByAgent == nil {
		t.Fatalf("stats maps should be initialized: %+v", st)
	}
	if st.DeliveryFailures < 0 || st.RedeliveryAttempts < 0 {
		t.Fatalf("counters should be non-negative: %+v", st)
	}
}
