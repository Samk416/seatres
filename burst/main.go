// Command burst reproduces an on-sale stampede against a running seatres
// service and prints the outcome distribution plus a final reconciliation.
//
//	go run ./burst -base https://your-app.example.com
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------- HTTP client

type client struct {
	base  string
	admin string
	hc    *http.Client
	sem   chan struct{} // caps in-flight requests
	reqs  atomic.Int64
}

type resp struct {
	status int
	body   map[string]any
	replay bool
	err    error
}

func newClient(base, admin string, conc int) *client {
	return &client{
		base:  strings.TrimRight(base, "/"),
		admin: admin,
		sem:   make(chan struct{}, conc),
		hc: &http.Client{
			Timeout: 90 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        conc,
				MaxIdleConnsPerHost: conc,
				MaxConnsPerHost:     conc,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (c *client) do(method, path, token string, body any) resp {
	c.sem <- struct{}{}
	defer func() { <-c.sem }()
	c.reqs.Add(1)

	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		return resp{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	r, err := c.hc.Do(req)
	if err != nil {
		return resp{err: err}
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	out := resp{status: r.StatusCode, replay: r.Header.Get("Idempotent-Replay") == "true"}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

func (c *client) getText(path string) string {
	c.sem <- struct{}{}
	defer func() { <-c.sem }()
	r, err := c.hc.Get(c.base + path)
	if err != nil {
		return ""
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return string(b)
}

// runAll starts n goroutines, holds them at a gate, then releases them all at once.
func runAll(n int, fn func(i int)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

// ------------------------------------------------------------ service helpers

func (c *client) waitReady() error {
	deadline := time.Now().Add(120 * time.Second)
	for {
		r := c.do("GET", "/healthz", "", nil)
		if r.status == 200 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("service not healthy after 120s (last status %d, err %v)", r.status, r.err)
		}
		fmt.Println("waiting for service (cold start?) ...")
		time.Sleep(3 * time.Second)
	}
}

func userName(prefix string, i int) string { return fmt.Sprintf("%s-%d", prefix, i) }

func seatNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "S" + strconv.Itoa(i+1)
	}
	return out
}

func (c *client) mintTokens(prefix string, n int) ([]string, error) {
	toks := make([]string, n)
	runAll(n, func(i int) {
		for try := 0; try < 5; try++ {
			r := c.do("POST", "/auth/token", "", map[string]any{"user_id": userName(prefix, i)})
			if r.status == 200 {
				toks[i], _ = r.body["token"].(string)
				return
			}
			time.Sleep(time.Duration(200*(try+1)) * time.Millisecond)
		}
	})
	for _, t := range toks {
		if t == "" {
			return nil, fmt.Errorf("could not mint all %d user tokens", n)
		}
	}
	return toks, nil
}

func (c *client) createShow(name string, seats []string) (string, error) {
	r := c.do("POST", "/shows", c.admin, map[string]any{
		"name": name, "seats": seats, "price_paise": 25000})
	switch {
	case r.status == 201:
		id, _ := r.body["id"].(string)
		return id, nil
	case r.status == 401 || r.status == 403:
		return "", fmt.Errorf("admin token rejected (status %d): pass -admin-token or set ADMIN_TOKEN", r.status)
	}
	return "", fmt.Errorf("create show failed: status %d, err %v, body %v", r.status, r.err, r.body)
}

type showState struct{ total, available, held, confirmed, limit int }

func (c *client) state(id string) (showState, error) {
	r := c.do("GET", "/shows/"+id, "", nil)
	if r.status != 200 {
		return showState{}, fmt.Errorf("GET /shows/%s: status %d, err %v", id, r.status, r.err)
	}
	n := func(k string) int { f, _ := r.body[k].(float64); return int(f) }
	return showState{n("total_seats"), n("available"), n("held"), n("confirmed"), n("per_user_limit")}, nil
}

func (c *client) scrape() map[string]float64 {
	m := map[string]float64{}
	for _, line := range strings.Split(c.getText("/metrics"), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndex(line, " ")
		if i < 0 {
			continue
		}
		if v, err := strconv.ParseFloat(line[i+1:], 64); err == nil {
			m[line[:i]] = v
		}
	}
	return m
}

// --------------------------------------------------------------------- phases

type result struct {
	desc   string
	ok     bool
	detail string
}

type phase struct {
	name    string
	showID  string
	total   int
	mu      sync.Mutex
	counts  map[string]int
	wins    map[string]string // seat -> reservation id that holds it
	bad     map[string]int    // violation kind -> count
	example map[string]string // violation kind -> first example
	anyRes  string
	checks  []result
	elapsed time.Duration
	reqs    int64
}

func newPhase(name string) *phase {
	return &phase{
		name:    name,
		counts:  map[string]int{},
		wins:    map[string]string{},
		bad:     map[string]int{},
		example: map[string]string{},
	}
}

func classify(r resp) string {
	switch {
	case r.err != nil || r.status == 0:
		return "client error (no response)"
	case r.status == 201 && r.replay:
		return "replay of earlier booking (201)"
	case r.status == 201:
		return "confirmed"
	case r.status >= 500:
		return fmt.Sprintf("5xx (%d)", r.status)
	case r.status == 409:
		reason, _ := r.body["error"].(string)
		return "declined: " + reason
	case r.status >= 400:
		reason, _ := r.body["error"].(string)
		return fmt.Sprintf("other 4xx (%d %s)", r.status, reason)
	}
	return fmt.Sprintf("unexpected (%d)", r.status)
}

func (p *phase) add(r resp) {
	k := classify(r)
	p.mu.Lock()
	p.counts[k]++
	p.mu.Unlock()
}

func (p *phase) count(k string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[k]
}

func (p *phase) countPrefix(prefix string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for k, v := range p.counts {
		if strings.HasPrefix(k, prefix) {
			n += v
		}
	}
	return n
}

func (p *phase) badLocked(kind, detail string) {
	p.bad[kind]++
	if _, ok := p.example[kind]; !ok {
		p.example[kind] = detail
	}
}

func (p *phase) violation(kind, detail string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.badLocked(kind, detail)
}

// record inspects a 201: identity must match the token, and no seat may
// ever belong to two different reservations.
func (p *phase) record(r resp, user string) {
	if r.status != 201 {
		return
	}
	id, _ := r.body["reservation_id"].(string)
	got, _ := r.body["user_id"].(string)
	seats, _ := r.body["seats"].([]any)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.anyRes == "" {
		p.anyRes = id
	}
	if got != user {
		p.badLocked("identity", fmt.Sprintf("sent as %q but reservation belongs to %q", user, got))
	}
	for _, s := range seats {
		seat, _ := s.(string)
		if prev, ok := p.wins[seat]; ok && prev != id {
			p.badLocked("double-sell", fmt.Sprintf("seat %s confirmed in reservations %s and %s", seat, prev, id))
		}
		p.wins[seat] = id
	}
}

func (p *phase) check(desc string, ok bool, detail string) {
	p.checks = append(p.checks, result{desc, ok, detail})
}

// finish adds the checks every phase must pass, including the final reconciliation.
func (c *client) finish(p *phase, t0 time.Time, reqs0 int64) {
	p.elapsed = time.Since(t0)
	p.reqs = c.reqs.Load() - reqs0

	p.check("zero 5xx responses", p.countPrefix("5xx") == 0,
		fmt.Sprintf("%d responses were 5xx", p.countPrefix("5xx")))
	p.check("every request received a response", p.count("client error (no response)") == 0,
		fmt.Sprintf("%d requests got no response (try a lower -concurrency)", p.count("client error (no response)")))
	p.check("no seat confirmed to two users", p.bad["double-sell"] == 0, p.example["double-sell"])
	p.check("identity comes from the token (spoofed body fields ignored)", p.bad["identity"] == 0, p.example["identity"])

	st, err := c.state(p.showID)
	if err != nil {
		p.check("final show state readable", false, err.Error())
		return
	}
	p.check("reconciliation: available + held + confirmed == total_seats",
		st.available+st.held+st.confirmed == st.total && st.total == p.total,
		fmt.Sprintf("api state %+v, expected total %d", st, p.total))
	p.check("reconciliation: confirmed seats in API == seats won in 201 responses",
		st.confirmed == len(p.wins),
		fmt.Sprintf("api says %d confirmed, 201 responses account for %d", st.confirmed, len(p.wins)))
}

func (p *phase) print() {
	fmt.Printf("\n== %s ==\n", p.name)
	keys := make([]string, 0, len(p.counts))
	for k := range p.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-46s %7d\n", k, p.counts[k])
	}
	secs := p.elapsed.Seconds()
	if secs <= 0 {
		secs = 0.001
	}
	fmt.Printf("  %d requests in %.1fs (%.0f req/s)\n", p.reqs, secs, float64(p.reqs)/secs)
	for _, r := range p.checks {
		if r.ok {
			fmt.Printf("  [PASS] %s\n", r.desc)
		} else {
			fmt.Printf("  [FAIL] %s  -> %s\n", r.desc, r.detail)
		}
	}
}

// Phase 1: many users, one seat.
func (c *client) hotSeatStorm(run string, n int) (*phase, error) {
	p := newPhase(fmt.Sprintf("Phase 1: hot-seat storm (%d users, 1 seat)", n))
	seats := seatNames(20)
	id, err := c.createShow("burst-hot-"+run, seats)
	if err != nil {
		return nil, err
	}
	p.showID, p.total = id, len(seats)
	prefix := run + "-hot"
	toks, err := c.mintTokens(prefix, n)
	if err != nil {
		return nil, err
	}

	t0, r0 := time.Now(), c.reqs.Load()
	runAll(n, func(i int) {
		r := c.do("POST", "/shows/"+id+"/reserve", toks[i], map[string]any{
			"seats": []string{"S12"}, "idempotency_key": fmt.Sprintf("%s-%d", prefix, i)})
		p.add(r)
		p.record(r, userName(prefix, i))
	})
	p.check("exactly one winner for the hot seat", p.count("confirmed") == 1,
		fmt.Sprintf("%d winners", p.count("confirmed")))
	p.check("everyone else got a clean 409 seat_taken", p.count("declined: seat_taken") == n-1,
		fmt.Sprintf("%d seat_taken, expected %d", p.count("declined: seat_taken"), n-1))
	c.finish(p, t0, r0)
	return p, nil
}

// Phase 2: the on-sale stampede.
func (c *client) stampede(run string, users, seatCount, hotSeats int) (*phase, error) {
	p := newPhase(fmt.Sprintf("Phase 2: on-sale stampede (%d users, %d seats, %d hot seats)", users, seatCount, hotSeats))
	seats := seatNames(seatCount)
	id, err := c.createShow("burst-sale-"+run, seats)
	if err != nil {
		return nil, err
	}
	p.showID, p.total = id, len(seats)
	prefix := run + "-sale"

	fmt.Printf("\npreparing %d user tokens ...\n", users)
	toks, err := c.mintTokens(prefix, users)
	if err != nil {
		return nil, err
	}
	path := "/shows/" + id + "/reserve"

	fmt.Println("firing the stampede ...")
	t0, r0 := time.Now(), c.reqs.Load()
	runAll(users, func(i int) {
		rng := rand.New(rand.NewSource(int64(i) + 1))
		user := userName(prefix, i)
		pick := func() string {
			if rng.Intn(100) < 60 {
				return seats[rng.Intn(hotSeats)] // 60% of traffic fights over the hot seats
			}
			return seats[rng.Intn(len(seats))]
		}
		want := []string{pick()}
		if rng.Intn(10) == 0 { // 10% ask for two seats
			if s := pick(); s != want[0] {
				want = append(want, s)
			}
		}
		key := fmt.Sprintf("%s-%d", prefix, i)
		body := map[string]any{"seats": want, "idempotency_key": key}
		if rng.Intn(10) == 0 {
			body["user_id"] = "spoofed-" + run // must be ignored by the server
		}

		r := c.do("POST", path, toks[i], body)
		p.add(r)
		p.record(r, user)

		if rng.Intn(5) == 0 { // 20% retry the identical request (lost response)
			r2 := c.do("POST", path, toks[i], body)
			p.add(r2)
			p.record(r2, user)
			if r.status == 201 {
				same := r2.status == 201 && r2.replay && r2.body["reservation_id"] == r.body["reservation_id"]
				if !same {
					p.violation("replay", fmt.Sprintf("retry returned status %d replay=%v", r2.status, r2.replay))
				}
			}
		}

		if r.status == 201 && rng.Intn(20) == 0 { // same key, different seats: must be refused
			alt := seats[rng.Intn(len(seats))]
			if len(want) != 1 || want[0] != alt {
				r3 := c.do("POST", path, toks[i], map[string]any{"seats": []string{alt}, "idempotency_key": key})
				p.add(r3)
				reason, _ := r3.body["error"].(string)
				if r3.status != 409 || reason != "idempotency_key_conflict" {
					p.violation("key-conflict", fmt.Sprintf("status %d reason %q", r3.status, reason))
				}
			}
		}
	})
	p.check("never more seats confirmed than seats for sale", len(p.wins) <= seatCount,
		fmt.Sprintf("%d seats won, only %d for sale", len(p.wins), seatCount))
	p.check("same-key retry returns the original reservation (Idempotent-Replay)", p.bad["replay"] == 0, p.example["replay"])
	p.check("same key + different seats is rejected with 409", p.bad["key-conflict"] == 0, p.example["key-conflict"])
	c.finish(p, t0, r0)
	return p, nil
}

// Phase 3: one greedy user fires parallel reserves; another user tries to cancel.
func (c *client) userLimit(run string) (*phase, error) {
	p := newPhase("Phase 3: per-user limit and ownership (1 user, parallel reserves)")
	seats := seatNames(50)
	id, err := c.createShow("burst-limit-"+run, seats)
	if err != nil {
		return nil, err
	}
	p.showID, p.total = id, len(seats)
	st0, err := c.state(id)
	if err != nil {
		return nil, err
	}
	limit := st0.limit
	parallel := limit + 6
	prefix := run + "-lim"
	toks, err := c.mintTokens(prefix, 2) // 0 = greedy user, 1 = other user
	if err != nil {
		return nil, err
	}

	t0, r0 := time.Now(), c.reqs.Load()
	runAll(parallel, func(i int) {
		r := c.do("POST", "/shows/"+id+"/reserve", toks[0], map[string]any{
			"seats":           []string{seats[i]},
			"idempotency_key": fmt.Sprintf("%s-%d", prefix, i),
			"user_id":         userName(prefix, 1), // spoof attempt: pretend to be the other user
		})
		p.add(r)
		p.record(r, userName(prefix, 0))
	})
	p.check(fmt.Sprintf("user ends with exactly the limit (%d) of seats", limit), p.count("confirmed") == limit,
		fmt.Sprintf("%d confirmed", p.count("confirmed")))
	p.check("the rest got a clean 409 per_user_limit", p.count("declined: per_user_limit") == parallel-limit,
		fmt.Sprintf("%d declined, expected %d", p.count("declined: per_user_limit"), parallel-limit))

	p.mu.Lock()
	rid := p.anyRes
	p.mu.Unlock()
	rc := c.do("POST", "/reservations/"+rid+"/cancel", toks[1], nil)
	p.add(rc)
	p.check("another user cannot cancel it (403)", rc.status == 403, fmt.Sprintf("got status %d", rc.status))

	c.finish(p, t0, r0)
	return p, nil
}

// ----------------------------------------------------------------------- main

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}

func main() {
	base := flag.String("base", env("BASE_URL", "http://localhost:8080"), "service base URL")
	admin := flag.String("admin-token", env("ADMIN_TOKEN", "admin-dev-token"), "admin token (or env ADMIN_TOKEN)")
	users := flag.Int("users", 20000, "users in the stampede")
	seatCount := flag.Int("seats", 2000, "seats in the stampede show")
	hotSeats := flag.Int("hot-seats", 10, "number of hot seats in the stampede")
	hotUsers := flag.Int("hot-users", 500, "users in the single-seat storm")
	conc := flag.Int("concurrency", 200, "max requests in flight from this client")
	flag.Parse()
	if flag.NArg() > 0 {
		*base = flag.Arg(0)
	}
	if *hotSeats > *seatCount {
		*hotSeats = *seatCount
	}
	if *hotSeats < 1 {
		*hotSeats = 1
	}

	c := newClient(*base, *admin, *conc)
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	fmt.Printf("seatres burst against %s (run %s, max %d in flight)\n", c.base, run, *conc)

	if err := c.waitReady(); err != nil {
		fatal(err)
	}
	before := c.scrape()

	var phases []*phase
	steps := []func() (*phase, error){
		func() (*phase, error) { return c.hotSeatStorm(run, *hotUsers) },
		func() (*phase, error) { return c.stampede(run, *users, *seatCount, *hotSeats) },
		func() (*phase, error) { return c.userLimit(run) },
	}
	for _, step := range steps {
		p, err := step()
		if err != nil {
			fatal(err)
		}
		p.print()
		phases = append(phases, p)
	}

	// Metrics must agree with what this run observed and with the API.
	after := c.scrape()
	newBookings, seatTaken := 0, 0
	for _, p := range phases {
		newBookings += p.count("confirmed")
		seatTaken += p.count("declined: seat_taken")
	}
	var mchecks []result
	delta := func(series string) (float64, bool) {
		a, ok := after[series]
		return a - before[series], ok
	}
	if d, ok := delta("reservations_confirmed_total"); !ok {
		mchecks = append(mchecks, result{"/metrics exposes reservations_confirmed_total", false, "series missing"})
	} else {
		mchecks = append(mchecks, result{"reservations_confirmed_total moved by the number of new bookings",
			int(d) == newBookings,
			fmt.Sprintf("metric moved by %.0f, run saw %d (other traffic on the server?)", d, newBookings)})
	}
	if d, ok := delta(`reservations_declined_total{reason="seat_taken"}`); !ok {
		mchecks = append(mchecks, result{"/metrics exposes reservations_declined_total{seat_taken}", false, "series missing"})
	} else {
		mchecks = append(mchecks, result{"reservations_declined_total{seat_taken} moved by the seat_taken declines",
			int(d) == seatTaken,
			fmt.Sprintf("metric moved by %.0f, run saw %d (other traffic on the server?)", d, seatTaken)})
	}
	for _, p := range phases {
		st, err := c.state(p.showID)
		if err != nil {
			mchecks = append(mchecks, result{"state readable for " + p.showID, false, err.Error()})
			continue
		}
		g, ok := after[fmt.Sprintf(`seats_available{show_id="%s"}`, p.showID)]
		g2 := c.scrape()[fmt.Sprintf(`seats_available{show_id="%s"}`, p.showID)]
		_ = g
		mchecks = append(mchecks, result{
			fmt.Sprintf("seats_available gauge == API available (%d) for %s", st.available, p.showID[:8]),
			ok && int(g2) == st.available,
			fmt.Sprintf("gauge %.0f (present=%v), API %d", g2, ok, st.available)})
	}
	fmt.Println("\n== Metrics reconciliation ==")
	for _, r := range mchecks {
		if r.ok {
			fmt.Printf("  [PASS] %s\n", r.desc)
		} else {
			fmt.Printf("  [FAIL] %s  -> %s\n", r.desc, r.detail)
		}
	}

	fails := 0
	for _, p := range phases {
		for _, r := range p.checks {
			if !r.ok {
				fails++
			}
		}
	}
	for _, r := range mchecks {
		if !r.ok {
			fails++
		}
	}
	fmt.Println()
	if fails == 0 {
		fmt.Println("RESULT: PASS (all checks passed)")
		return
	}
	fmt.Printf("RESULT: FAIL (%d checks failed)\n", fails)
	os.Exit(1)
}
