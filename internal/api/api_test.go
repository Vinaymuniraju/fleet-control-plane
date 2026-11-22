package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/store"
)

type recorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *recorder) notify(id string) { r.mu.Lock(); r.ids = append(r.ids, id); r.mu.Unlock() }

func newTestServer(t *testing.T) (*httptest.Server, *store.Memory, *recorder) {
	t.Helper()
	st := store.NewMemory()
	rec := &recorder{}
	srv := httptest.NewServer(New(st, rec.notify, nil, nil).Handler())
	t.Cleanup(srv.Close)
	return srv, st, rec
}

func do(t *testing.T, method, url string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if s, ok := body.(string); ok {
			buf.WriteString(s)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, _ := http.NewRequest(method, url, &buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func kongReq(name string) CreateRequest {
	return CreateRequest{
		Name:   name,
		Engine: model.EngineKong,
		Kong: &model.KongSpec{Routes: []model.Route{
			{Name: "echo", Path: "/echo", Upstream: "http://httpbin.org"},
		}},
	}
}

func TestCreateAppliesDefaultsAndNotifies(t *testing.T) {
	srv, _, rec := newTestServer(t)
	resp, body := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	spec := body["spec"].(map[string]any)
	if spec["version"] != "3.9" || spec["replicas"].(float64) != 1 {
		t.Fatalf("defaults not applied: %v", spec)
	}
	if body["status"].(map[string]any)["phase"] != string(model.PhasePending) {
		t.Fatalf("want Pending phase, got %v", body["status"])
	}
	if len(rec.ids) != 1 || rec.ids[0] != body["id"] {
		t.Fatalf("reconciler not notified: %v", rec.ids)
	}
}

func TestCreateValidation(t *testing.T) {
	srv, _, _ := newTestServer(t)
	cases := []struct {
		name string
		body any
	}{
		{"bad name", CreateRequest{Name: "Bad_Name", Engine: model.EngineKong}},
		{"unknown engine", CreateRequest{Name: "x", Engine: "redis"}},
		{"too many replicas", CreateRequest{Name: "x", Engine: model.EngineKong, Replicas: 99}},
		{"route path", CreateRequest{Name: "x", Engine: model.EngineKong, Kong: &model.KongSpec{Routes: []model.Route{{Name: "a", Path: "nope", Upstream: "http://a"}}}}},
		{"route upstream", CreateRequest{Name: "x", Engine: model.EngineKong, Kong: &model.KongSpec{Routes: []model.Route{{Name: "a", Path: "/a", Upstream: "ftp://a"}}}}},
		{"duplicate route", CreateRequest{Name: "x", Engine: model.EngineKong, Kong: &model.KongSpec{Routes: []model.Route{{Name: "a", Path: "/a", Upstream: "http://a"}, {Name: "a", Path: "/b", Upstream: "http://b"}}}}},
		{"plugin not allowed", CreateRequest{Name: "x", Engine: model.EngineKong, Kong: &model.KongSpec{Plugins: []model.Plugin{{Name: "pre-function"}}}}},
		{"pg setting not tunable", CreateRequest{Name: "x", Engine: model.EnginePostgres, Postgres: &model.PostgresSpec{Settings: map[string]string{"wal_level": "minimal"}}}},
		{"pg setting injection", CreateRequest{Name: "x", Engine: model.EnginePostgres, Postgres: &model.PostgresSpec{Settings: map[string]string{"work_mem": "4MB'; DROP"}}}},
		{"pg extension", CreateRequest{Name: "x", Engine: model.EnginePostgres, Postgres: &model.PostgresSpec{Extensions: []string{"plpython3u"}}}},
		{"mixed specs", CreateRequest{Name: "x", Engine: model.EngineKong, Postgres: &model.PostgresSpec{}}},
		{"unknown field", `{"name":"x","engine":"kong","replica":2}`},
		{"bad json", `{`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := do(t, "POST", srv.URL+"/v1/services", tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %v", resp.StatusCode, body)
			}
		})
	}
}

func TestDuplicateNameConflicts(t *testing.T) {
	srv, _, _ := newTestServer(t)
	do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	resp, _ := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("want 409, got %d", resp.StatusCode)
	}
}

func TestPatchBumpsGenerationAndLogsEvent(t *testing.T) {
	srv, _, rec := newTestServer(t)
	_, created := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	id := created["id"].(string)

	resp, body := do(t, "PATCH", srv.URL+"/v1/services/"+id, map[string]any{"replicas": 3, "version": "3.10"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	if body["generation"].(float64) != 2 {
		t.Fatalf("generation not bumped: %v", body["generation"])
	}
	_, ev := do(t, "GET", srv.URL+"/v1/services/"+id+"/events", nil)
	items := ev["items"].([]any)
	last := items[len(items)-1].(map[string]any)
	if last["reason"] != "SpecChanged" || last["message"] != "version 3.9 -> 3.10; replicas 1 -> 3" {
		t.Fatalf("unexpected event: %v", last)
	}
	if len(rec.ids) != 2 {
		t.Fatalf("want 2 notifications, got %d", len(rec.ids))
	}
}

func TestPatchWithStaleResourceVersionConflicts(t *testing.T) {
	srv, st, _ := newTestServer(t)
	_, created := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	id := created["id"].(string)

	// Simulate the reconciler writing status in between.
	svc, _ := st.Get(t.Context(), id)
	svc.Status.Phase = model.PhaseReady
	if err := st.UpdateStatus(t.Context(), svc); err != nil {
		t.Fatal(err)
	}

	resp, _ := do(t, "PATCH", srv.URL+"/v1/services/"+id, map[string]any{"resourceVersion": 1, "replicas": 2})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("want 409 for stale resourceVersion, got %d", resp.StatusCode)
	}
	// Without a resourceVersion the server retries and wins.
	resp, _ = do(t, "PATCH", srv.URL+"/v1/services/"+id, map[string]any{"replicas": 2})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
}

func TestPatchInvalidKeepsOldSpec(t *testing.T) {
	srv, st, _ := newTestServer(t)
	_, created := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	id := created["id"].(string)
	resp, _ := do(t, "PATCH", srv.URL+"/v1/services/"+id, map[string]any{"replicas": 0})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
	svc, _ := st.Get(t.Context(), id)
	if svc.Spec.Replicas != 1 {
		t.Fatalf("spec changed by an invalid patch: %d", svc.Spec.Replicas)
	}
}

func TestDeleteMarksDeletingAndBlocksPatch(t *testing.T) {
	srv, _, _ := newTestServer(t)
	_, created := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	id := created["id"].(string)

	resp, body := do(t, "DELETE", srv.URL+"/v1/services/"+id, nil)
	if resp.StatusCode != http.StatusAccepted || body["deleting"] != true {
		t.Fatalf("want 202 deleting, got %d %v", resp.StatusCode, body)
	}
	resp, _ = do(t, "PATCH", srv.URL+"/v1/services/"+id, map[string]any{"replicas": 2})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("patch on deleting service: want 409, got %d", resp.StatusCode)
	}
	// Idempotent.
	resp, _ = do(t, "DELETE", srv.URL+"/v1/services/"+id, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("second delete: want 202, got %d", resp.StatusCode)
	}
}

func TestNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	for _, m := range []string{"GET", "DELETE"} {
		resp, _ := do(t, m, srv.URL+"/v1/services/svc-missing", nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: want 404, got %d", m, resp.StatusCode)
		}
	}
}

func TestFailoverOnlyForPostgres(t *testing.T) {
	srv, _, _ := newTestServer(t)
	_, created := do(t, "POST", srv.URL+"/v1/services", kongReq("edge"))
	resp, _ := do(t, "POST", srv.URL+"/v1/services/"+created["id"].(string)+"/failover", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func TestListOrdersByCreation(t *testing.T) {
	srv, _, _ := newTestServer(t)
	do(t, "POST", srv.URL+"/v1/services", kongReq("a"))
	do(t, "POST", srv.URL+"/v1/services", CreateRequest{Name: "b", Engine: model.EnginePostgres})
	_, body := do(t, "GET", srv.URL+"/v1/services", nil)
	items := body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
}
