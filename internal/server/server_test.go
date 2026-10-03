package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Samk416/seatres/internal/auth"
	"github.com/Samk416/seatres/internal/db"
	"github.com/Samk416/seatres/internal/server"
)

const adminToken = "test-admin"

type env struct {
	base    string
	a       *auth.Auth
	client  *http.Client
	sem     chan struct{}
	errOnce sync.Once
}

func newEnv(t *testing.T) *env {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://seat:seat@localhost:5432/seatres"
	}
	pool, err := db.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("cannot reach database (is docker compose up?): %v", err)
	}
	t.Cleanup(pool.Close)

	a := &auth.Auth{Secret: []byte("test-secret"), AdminToken: adminToken}
	app := server.New(pool, a)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go app.Listener(ln)
	t.Cleanup(func() { _ = app.Shutdown() })

	return &env{
		base: "http://" + ln.Addr().String(),
		a:    a,
		sem:  make(chan struct{}, 100), // at most 100 requests in flight
		client: &http.Client{
			Timeout:   60 * time.Second,
			Transport: &http.Transport{MaxIdleConns: 2000, MaxIdleConnsPerHost: 2000},
		},
	}
}

// do sends a request. Status 0 means a network-level failure.
func (e *env) do(method, path, token string, body any) (int, map[string]any) {

	e.sem <- struct{}{}
	defer func() { <-e.sem }()

	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, e.base+path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.errOnce.Do(func() { fmt.Println("CLIENT ERROR:", err) })
		return 0, nil
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *env) createShow(t *testing.T, seats []string) string {
	t.Helper()
	code, out := e.do("POST", "/shows", adminToken,
		map[string]any{"name": "test", "seats": seats, "price_paise": 25000})
	if code != 201 {
		t.Fatalf("create show: status %d, body %v", code, out)
	}
	return out["id"].(string)
}

type state struct{ total, available, held, confirmed int }

func (e *env) state(t *testing.T, showID string) state {
	t.Helper()
	code, out := e.do("GET", "/shows/"+showID, "", nil)
	if code != 200 {
		t.Fatalf("get show: status %d", code)
	}
	n := func(k string) int { return int(out[k].(float64)) }
	return state{n("total_seats"), n("available"), n("held"), n("confirmed")}
}

// storm fires n users at once; each asks for the seats pick(i) returns.
func (e *env) storm(showID string, n int, pick func(i int) []string) []int {
	codes := make([]int, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tok, _ := e.a.Token(fmt.Sprintf("user-%d", i))
			<-start // everyone waits here, then all go at the same moment
			codes[i], _ = e.do("POST", "/shows/"+showID+"/reserve", tok, map[string]any{
				"seats":           pick(i),
				"idempotency_key": fmt.Sprintf("key-%d", i),
			})
		}(i)
	}
	close(start)
	wg.Wait()
	return codes
}

func tally(codes []int) map[int]int {
	m := map[int]int{}
	for _, c := range codes {
		m[c]++
	}
	return m
}

// 500 different users all grab seat A1: exactly one winner.
func TestHotSeatStorm(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"})

	const n = 500
	tc := tally(e.storm(id, n, func(int) []string { return []string{"A1"} }))

	if tc[201] != 1 || tc[409] != n-1 {
		t.Fatalf("want 1x201 and %dx409, got %v", n-1, tc)
	}
	st := e.state(t, id)
	if st.confirmed != 1 || st.available != 9 || st.available+st.held+st.confirmed != st.total {
		t.Fatalf("bad state: %+v", st)
	}
}

// Overlapping multi-seat requests in opposite orders:
// no deadlock (no 5xx), and no half-booked requests.
func TestMultiSeatAllOrNothing(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2", "A3", "A4"})
	pairs := [][]string{{"A1", "A2"}, {"A2", "A1"}, {"A2", "A3"}, {"A3", "A4"}, {"A4", "A1"}, {"A3", "A2"}}

	const n = 300
	tc := tally(e.storm(id, n, func(i int) []string { return pairs[i%len(pairs)] }))

	if tc[201]+tc[409] != n {
		t.Fatalf("unexpected statuses (5xx or network failure?): %v", tc)
	}
	if tc[201] < 1 {
		t.Fatalf("expected at least one winner: %v", tc)
	}
	st := e.state(t, id)
	if st.confirmed != 2*tc[201] {
		t.Fatalf("half-booked request! winners=%d confirmed seats=%d", tc[201], st.confirmed)
	}
	if st.available+st.held+st.confirmed != st.total {
		t.Fatalf("invariant broken: %+v", st)
	}
}
