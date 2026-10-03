package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
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
	runID   string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://seat:seat@localhost:5432/seatres"
	}
	pool, err := db.NewPool(context.Background(), url)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(pool.Close)

	a := &auth.Auth{Secret: []byte("test-secret"), AdminToken: adminToken}
	app := server.New(pool, a)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go app.Listener(ln)

	e := &env{
		base:  "http://" + ln.Addr().String(),
		a:     a,
		sem:   make(chan struct{}, 100), // at most 100 requests in flight
		runID: strconv.FormatInt(time.Now().UnixNano(), 36),
		client: &http.Client{
			Timeout:   60 * time.Second,
			Transport: &http.Transport{MaxIdleConns: 2000, MaxIdleConnsPerHost: 2000},
		},
	}
	t.Cleanup(func() {
		e.client.CloseIdleConnections()              // client drops its keep-alive connections first
		_ = app.ShutdownWithTimeout(3 * time.Second) // and shutdown can never wait forever
	})
	return e

}

// do sends a request. Status 0 means a network-level failure.
// do sends a request. Status 0 means a network-level failure.
func (e *env) do(method, path, token string, body any) (int, map[string]any) {
	e.sem <- struct{}{}
	defer func() { <-e.sem }()

	// Make idempotency keys unique per test run (the DB persists between runs).
	if m, ok := body.(map[string]any); ok {
		if k, ok := m["idempotency_key"].(string); ok {
			cp := make(map[string]any, len(m))
			for kk, vv := range m {
				cp[kk] = vv
			}
			cp["idempotency_key"] = k + "-" + e.runID
			body = cp
		}
	}

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

// One user fires 10 parallel single-seat requests with limit=4:
// exactly 4 succeed, 6 are declined, and the user ends with 4 seats.
func TestPerUserLimit(t *testing.T) {
	e := newEnv(t)
	seats := []string{"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"}
	id := e.createShow(t, seats)
	tok, _ := e.a.Token("greedy-user")

	codes := make([]int, len(seats))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, s := range seats {
		wg.Add(1)
		go func(i int, s string) {
			defer wg.Done()
			<-start
			codes[i], _ = e.do("POST", "/shows/"+id+"/reserve", tok, map[string]any{
				"seats":           []string{s},
				"idempotency_key": fmt.Sprintf("limit-key-%d", i),
			})
		}(i, s)
	}
	close(start)
	wg.Wait()

	tc := tally(codes)
	if tc[201] != 4 || tc[409] != 6 {
		t.Fatalf("want 4x201 and 6x409, got %v", tc)
	}
	st := e.state(t, id)
	if st.confirmed != 4 || st.available != 6 || st.available+st.held+st.confirmed != st.total {
		t.Fatalf("bad state: %+v", st)
	}
}

// Same key + same body = same reservation, nothing extra moves.
// Same key + different seats = 409.
func TestIdempotency(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2", "A3", "A4"})
	tok, _ := e.a.Token("retry-user")
	path := "/shows/" + id + "/reserve"

	c1, r1 := e.do("POST", path, tok, map[string]any{"seats": []string{"A1"}, "idempotency_key": "K1"})
	if c1 != 201 {
		t.Fatalf("first request: want 201, got %d", c1)
	}
	c2, r2 := e.do("POST", path, tok, map[string]any{"seats": []string{"A1"}, "idempotency_key": "K1"})
	if c2 != 201 || r2["reservation_id"] != r1["reservation_id"] {
		t.Fatalf("retry: want 201 with same reservation, got %d %v vs %v", c2, r2, r1)
	}
	c3, _ := e.do("POST", path, tok, map[string]any{"seats": []string{"A2"}, "idempotency_key": "K1"})
	if c3 != 409 {
		t.Fatalf("same key, different seats: want 409, got %d", c3)
	}
	st := e.state(t, id)
	if st.confirmed != 1 || st.available != 3 {
		t.Fatalf("retries moved something: %+v", st)
	}
}

// 50 simultaneous retries with the same key: one real booking, 49 replays.
func TestIdempotencyConcurrent(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2"})
	tok, _ := e.a.Token("retry-storm-user")

	const n = 50
	codes := make([]int, n)
	ids := make([]any, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var out map[string]any
			codes[i], out = e.do("POST", "/shows/"+id+"/reserve", tok,
				map[string]any{"seats": []string{"A1"}, "idempotency_key": "same-key"})
			if out != nil {
				ids[i] = out["reservation_id"]
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if tc := tally(codes); tc[201] != n {
		t.Fatalf("want %dx201, got %v", n, tc)
	}
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("different reservation ids: %v vs %v", ids[i], ids[0])
		}
	}
	st := e.state(t, id)
	if st.confirmed != 1 || st.available != 1 {
		t.Fatalf("expected exactly one seat booked: %+v", st)
	}
}

func (e *env) reserveSeat(showID, token, key string, seats ...string) (int, string) {
	code, out := e.do("POST", "/shows/"+showID+"/reserve", token,
		map[string]any{"seats": seats, "idempotency_key": key})
	id, _ := out["reservation_id"].(string)
	return code, id
}

// Owner-only cancel, rebook after cancel, and a stale cancel must never
// free a seat that now belongs to someone else.
func TestCancelOwnerOnlyAndNoResurrection(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2"})
	alice, _ := e.a.Token("alice")
	bob, _ := e.a.Token("bob")

	code, resA := e.reserveSeat(id, alice, "ka", "A1")
	if code != 201 {
		t.Fatalf("alice reserve: want 201, got %d", code)
	}
	cancel := "/reservations/" + resA + "/cancel"

	if c, _ := e.do("POST", cancel, bob, nil); c != 403 {
		t.Fatalf("bob cancelling alice's reservation: want 403, got %d", c)
	}
	if c, _ := e.do("POST", cancel, "", nil); c != 401 {
		t.Fatalf("no token: want 401, got %d", c)
	}
	if st := e.state(t, id); st.confirmed != 1 {
		t.Fatalf("seat must still be confirmed: %+v", st)
	}
	if c, _ := e.do("POST", "/reservations/00000000-0000-0000-0000-000000000000/cancel", alice, nil); c != 404 {
		t.Fatalf("unknown reservation: want 404, got %d", c)
	}
	if c, _ := e.do("POST", "/reservations/abc/cancel", alice, nil); c != 400 {
		t.Fatalf("bad id: want 400, got %d", c)
	}

	if c, _ := e.do("POST", cancel, alice, nil); c != 200 {
		t.Fatalf("alice cancel: want 200, got %d", c)
	}
	if st := e.state(t, id); st.confirmed != 0 || st.available != 2 {
		t.Fatalf("seat should be free again: %+v", st)
	}

	// The released seat is re-bookable.
	if code, _ := e.reserveSeat(id, bob, "kb", "A1"); code != 201 {
		t.Fatalf("bob rebook: want 201, got %d", code)
	}

	// Alice retries her old cancel: succeeds, but must NOT free bob's seat.
	if c, _ := e.do("POST", cancel, alice, nil); c != 200 {
		t.Fatalf("stale cancel: want 200, got %d", c)
	}
	if st := e.state(t, id); st.confirmed != 1 || st.available != 1 {
		t.Fatalf("stale cancel resurrected a seat: %+v", st)
	}
}

// 30 parallel cancels of one reservation racing 30 users trying to rebook
// one of its seats: no 5xx, at most one rebooker wins, invariant holds.
func TestCancelRaces(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2"})
	owner, _ := e.a.Token("owner")
	code, resID := e.reserveSeat(id, owner, "k-owner", "A1", "A2")
	if code != 201 {
		t.Fatalf("owner reserve: want 201, got %d", code)
	}

	const n = 30
	cancelCodes := make([]int, n)
	rebookCodes := make([]int, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			<-start
			cancelCodes[i], _ = e.do("POST", "/reservations/"+resID+"/cancel", owner, nil)
		}(i)
		go func(i int) {
			defer wg.Done()
			tok, _ := e.a.Token(fmt.Sprintf("rb-%d", i))
			<-start
			rebookCodes[i], _ = e.do("POST", "/shows/"+id+"/reserve", tok, map[string]any{
				"seats":           []string{"A1"},
				"idempotency_key": fmt.Sprintf("rb-key-%d", i),
			})
		}(i)
	}
	close(start)
	wg.Wait()

	if cc := tally(cancelCodes); cc[200] != n {
		t.Fatalf("want %dx200 for cancels, got %v", n, cc)
	}
	rc := tally(rebookCodes)
	if rc[201] > 1 || rc[201]+rc[409] != n {
		t.Fatalf("rebook outcomes wrong: %v", rc)
	}
	st := e.state(t, id)
	if st.confirmed != rc[201] || st.available+st.held+st.confirmed != st.total {
		t.Fatalf("bad state: %+v (rebook wins: %d)", st, rc[201])
	}
}

func (e *env) raw(method, path, token, body string) int {
	req, _ := http.NewRequest(method, e.base+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// Nasty input must always produce a clean 4xx, never a 5xx.
func TestBadInputNever5xx(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2"})
	tok, _ := e.a.Token("fuzz-user")
	reserve := "/shows/" + id + "/reserve"
	long := strings.Repeat("x", 300)
	many := `"S1","S2","S3","S4","S5","S6","S7","S8","S9","S10","S11","S12","S13","S14","S15","S16","S17","S18","S19","S20","S21"`
	admin := "/shows"

	cases := []struct {
		name, method, path, token, body string
		want                            int // 0 = any 4xx
	}{
		{"invalid json", "POST", reserve, tok, `{`, 400},
		{"empty body", "POST", reserve, tok, ``, 400},
		{"seats wrong type", "POST", reserve, tok, `{"seats":"A1","idempotency_key":"k"}`, 400},
		{"no idempotency key", "POST", reserve, tok, `{"seats":["A1"]}`, 400},
		{"NUL in key", "POST", reserve, tok, `{"seats":["A1"],"idempotency_key":"a\u0000b"}`, 400},
		{"key too long", "POST", reserve, tok, `{"seats":["A1"],"idempotency_key":"` + long + `"}`, 400},
		{"empty seats", "POST", reserve, tok, `{"seats":[],"idempotency_key":"k"}`, 400},
		{"null seat", "POST", reserve, tok, `{"seats":[null],"idempotency_key":"k"}`, 400},
		{"NUL in seat", "POST", reserve, tok, `{"seats":["A\u00001"],"idempotency_key":"k"}`, 400},
		{"seat too long", "POST", reserve, tok, `{"seats":["` + long + `"],"idempotency_key":"k"}`, 400},
		{"duplicate seats", "POST", reserve, tok, `{"seats":["A1","A1"],"idempotency_key":"k"}`, 400},
		{"21 seats", "POST", reserve, tok, `{"seats":[` + many + `],"idempotency_key":"k"}`, 400},
		{"bad show id", "POST", "/shows/abc/reserve", tok, `{"seats":["A1"],"idempotency_key":"k"}`, 400},
		{"unknown show", "POST", "/shows/22222222-2222-2222-2222-222222222222/reserve", tok, `{"seats":["A1"],"idempotency_key":"k"}`, 404},
		{"unknown seat", "POST", reserve, tok, `{"seats":["Z99"],"idempotency_key":"k"}`, 404},
		{"no token", "POST", reserve, "", `{"seats":["A1"],"idempotency_key":"k"}`, 401},
		{"garbage token", "POST", reserve, "not-a-jwt", `{"seats":["A1"],"idempotency_key":"k"}`, 401},
		{"user token on admin route", "POST", admin, tok, `{"name":"x","seats":["A1"],"price_paise":1}`, 403},
		{"admin: NUL in name", "POST", admin, adminToken, `{"name":"a\u0000b","seats":["A1"],"price_paise":1}`, 400},
		{"admin: negative price", "POST", admin, adminToken, `{"name":"x","seats":["A1"],"price_paise":-5}`, 400},
		{"admin: float price", "POST", admin, adminToken, `{"name":"x","seats":["A1"],"price_paise":1e30}`, 400},
		{"admin: max int64 price", "POST", admin, adminToken, `{"name":"x","seats":["A1"],"price_paise":9223372036854775807}`, 400},
		{"admin: no seats", "POST", admin, adminToken, `{"name":"x","seats":[],"price_paise":1}`, 400},
		{"admin: empty seat name", "POST", admin, adminToken, `{"name":"x","seats":[""],"price_paise":1}`, 400},
		{"cancel bad id", "POST", "/reservations/abc/cancel", tok, ``, 400},
		{"unknown route", "GET", "/nope", "", ``, 404},
		{"wrong method", "DELETE", "/shows", "", ``, 0},
		{"token: empty user", "POST", "/auth/token", "", `{"user_id":""}`, 400},
		{"token: NUL user", "POST", "/auth/token", "", `{"user_id":"a\u0000b"}`, 400},
	}
	for _, tc := range cases {
		got := e.raw(tc.method, tc.path, tc.token, tc.body)
		if tc.want != 0 && got != tc.want {
			t.Errorf("%s: want %d, got %d", tc.name, tc.want, got)
		} else if tc.want == 0 && (got < 400 || got > 499) {
			t.Errorf("%s: want a 4xx, got %d", tc.name, got)
		}
	}
	if st := e.state(t, id); st.available != 2 || st.confirmed != 0 {
		t.Fatalf("bad input must not change state: %+v", st)
	}
}

// Everything at once: reserves, same-key retries, cancels, rebooks and reads.
// Pass = zero 5xx, zero dropped requests, invariant holds.
func TestMixedLoadNoServerErrors(t *testing.T) {
	e := newEnv(t)
	const totalSeats = 120
	seats := make([]string, totalSeats)
	for i := range seats {
		seats[i] = fmt.Sprintf("S%d", i)
	}
	id := e.createShow(t, seats)
	path := "/shows/" + id + "/reserve"

	const users = 400
	var mu sync.Mutex
	codes := map[int]int{}
	record := func(c int) {
		mu.Lock()
		codes[c]++
		mu.Unlock()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for u := 0; u < users; u++ {
		wg.Add(1)
		go func(u int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(u)))
			tok, _ := e.a.Token(fmt.Sprintf("chaos-%d", u))
			pick := func() []string {
				n := 1 + rng.Intn(3)
				set := map[string]bool{}
				for len(set) < n {
					i := rng.Intn(totalSeats)
					if rng.Intn(10) < 7 {
						i = rng.Intn(10) // 70% of traffic fights over 10 hot seats
					}
					set[seats[i]] = true
				}
				out := make([]string, 0, n)
				for s := range set {
					out = append(out, s)
				}
				return out
			}

			<-start
			body := map[string]any{"seats": pick(), "idempotency_key": "c1"}
			c, out := e.do("POST", path, tok, body)
			record(c)
			c, _ = e.do("POST", path, tok, body) // client retry, same key
			record(c)
			if rid, ok := out["reservation_id"].(string); ok && rng.Intn(2) == 0 {
				c, _ = e.do("POST", "/reservations/"+rid+"/cancel", tok, nil)
				record(c)
			}
			c, _ = e.do("POST", path, tok, map[string]any{"seats": pick(), "idempotency_key": "c2"})
			record(c)
			c, _ = e.do("GET", "/shows/"+id, "", nil)
			record(c)
		}(u)
	}
	close(start)
	wg.Wait()

	for code, n := range codes {
		if code != 200 && code != 201 && code != 409 {
			t.Errorf("unexpected status %d (x%d)", code, n)
		}
	}
	st := e.state(t, id)
	if st.total != totalSeats || st.available+st.held+st.confirmed != st.total {
		t.Fatalf("invariant broken: %+v", st)
	}
	t.Logf("outcomes: %v  final state: %+v", codes, st)
}

func (e *env) get(path string) string {
	resp, err := e.client.Get(e.base + path)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// metricValue reads one series from /metrics, e.g.
// `reservations_declined_total{reason="seat_taken"}`. Missing series = 0.
func (e *env) metricValue(t *testing.T, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(e.get("/metrics"), "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
			if err != nil {
				t.Fatalf("bad metric line %q", line)
			}
			return v
		}
	}
	return 0
}

// Metrics must agree with what the API did and with the API's own state.
func TestMetricsReconcileWithAPI(t *testing.T) {
	e := newEnv(t)
	id := e.createShow(t, []string{"A1", "A2", "A3"})
	path := "/shows/" + id + "/reserve"
	u1, _ := e.a.Token("m-user-1")
	u2, _ := e.a.Token("m-user-2")

	conf := "reservations_confirmed_total"
	taken := `reservations_declined_total{reason="seat_taken"}`
	replay := `reservations_declined_total{reason="idempotent_replay"}`
	c0, t0, r0 := e.metricValue(t, conf), e.metricValue(t, taken), e.metricValue(t, replay)

	body := map[string]any{"seats": []string{"A1"}, "idempotency_key": "m1"}
	if c, _ := e.do("POST", path, u1, body); c != 201 {
		t.Fatalf("first reserve: want 201, got %d", c)
	}
	if c, _ := e.do("POST", path, u2, map[string]any{"seats": []string{"A1"}, "idempotency_key": "m2"}); c != 409 {
		t.Fatalf("second user, same seat: want 409, got %d", c)
	}
	if c, _ := e.do("POST", path, u1, body); c != 201 { // same key = replay
		t.Fatalf("replay: want 201, got %d", c)
	}

	if d := e.metricValue(t, conf) - c0; d != 1 {
		t.Fatalf("confirmed counter moved by %v, want 1", d)
	}
	if d := e.metricValue(t, taken) - t0; d != 1 {
		t.Fatalf("seat_taken counter moved by %v, want 1", d)
	}
	if d := e.metricValue(t, replay) - r0; d != 1 {
		t.Fatalf("idempotent_replay counter moved by %v, want 1", d)
	}

	st := e.state(t, id)
	avail := e.metricValue(t, fmt.Sprintf(`seats_available{show_id="%s"}`, id))
	if int(avail) != st.available || st.available != 2 {
		t.Fatalf("seats_available gauge %v vs API %+v", avail, st)
	}
}

// Liveness never depends on the DB. Readiness fails closed when the DB is gone.
func TestHealthAndReadiness(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://seat:seat@localhost:5432/seatres"
	}
	pool, err := db.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("cannot reach database: %v", err)
	}
	defer pool.Close()
	app := server.New(pool, &auth.Auth{Secret: []byte("s"), AdminToken: "a"})

	status := func(path string) int {
		resp, err := app.Test(httptest.NewRequest("GET", path, nil), 5000)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if c := status("/healthz"); c != 200 {
		t.Fatalf("/healthz: want 200, got %d", c)
	}
	if c := status("/readyz"); c != 200 {
		t.Fatalf("/readyz with DB up: want 200, got %d", c)
	}

	pool.Close() // simulate the database going away
	if c := status("/healthz"); c != 200 {
		t.Fatalf("/healthz must not depend on the DB, got %d", c)
	}
	if c := status("/readyz"); c != 503 {
		t.Fatalf("/readyz must fail closed (503) when the DB is down, got %d", c)
	}
}
