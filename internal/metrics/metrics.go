package metrics

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	Confirmed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reservations_confirmed_total",
		Help: "Reservations successfully created (new bookings only).",
	})
	Declined = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "reservations_declined_total",
		Help: "Reserve requests that did not create a new booking, by reason.",
	}, []string{"reason"})
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests by route pattern and status code.",
	}, []string{"route", "status"})
	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route pattern.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"route"})
)

func init() {
	// Create the series at zero so dashboards show them before the first event.
	for _, r := range []string{"seat_taken", "per_user_limit", "idempotent_replay",
		"idempotency_key_conflict", "unknown_seat", "show_not_found"} {
		Declined.WithLabelValues(r)
	}
}

// NewRegistry builds a registry for one server instance.
func NewRegistry(db *pgxpool.Pool) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		Confirmed, Declined, HTTPRequests, HTTPDuration,
		newDBCollector(db),
	)
	return reg
}

// dbCollector reads seat counts and pool stats at scrape time.
type dbCollector struct {
	db                                 *pgxpool.Pool
	available, held, confirmed         *prometheus.Desc
	poolAcquired, poolMax, poolWaiting *prometheus.Desc
}

func newDBCollector(db *pgxpool.Pool) *dbCollector {
	lbl := []string{"show_id"}
	return &dbCollector{
		db:          db,
		available:   prometheus.NewDesc("seats_available", "Available seats per show (read from the DB at scrape time; 20 most recent shows).", lbl, nil),
		held:        prometheus.NewDesc("seats_held", "Held seats per show.", lbl, nil),
		confirmed:   prometheus.NewDesc("seats_confirmed", "Confirmed seats per show.", lbl, nil),
		poolAcquired: prometheus.NewDesc("db_pool_acquired_conns", "DB connections currently in use.", nil, nil),
		poolMax:     prometheus.NewDesc("db_pool_max_conns", "Configured maximum DB connections.", nil, nil),
		poolWaiting: prometheus.NewDesc("db_pool_empty_acquires_total", "Times a request had to wait because every connection was busy.", nil, nil),
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.available
	ch <- c.held
	ch <- c.confirmed
	ch <- c.poolAcquired
	ch <- c.poolMax
	ch <- c.poolWaiting
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.db.Stat()
	ch <- prometheus.MustNewConstMetric(c.poolAcquired, prometheus.GaugeValue, float64(st.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.poolMax, prometheus.GaugeValue, float64(st.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.poolWaiting, prometheus.CounterValue, float64(st.EmptyAcquireCount()))

	// One statement = one consistent snapshot, so the three states always add up.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := c.db.Query(ctx,
		`SELECT show_id::text, status, count(*)
		   FROM seats
		  WHERE show_id IN (SELECT id FROM shows ORDER BY created_at DESC LIMIT 20)
		  GROUP BY show_id, status`)
	if err != nil {
		return // skip the seat gauges this scrape; never fail the whole scrape
	}
	defer rows.Close()

	type counts struct{ a, h, c float64 }
	byShow := map[string]*counts{}
	for rows.Next() {
		var id, status string
		var n int64
		if rows.Scan(&id, &status, &n) != nil {
			return
		}
		cc := byShow[id]
		if cc == nil {
			cc = &counts{}
			byShow[id] = cc
		}
		switch status {
		case "available":
			cc.a = float64(n)
		case "held":
			cc.h = float64(n)
		case "confirmed":
			cc.c = float64(n)
		}
	}
	if rows.Err() != nil {
		return
	}
	for id, cc := range byShow {
		ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, cc.a, id)
		ch <- prometheus.MustNewConstMetric(c.held, prometheus.GaugeValue, cc.h, id)
		ch <- prometheus.MustNewConstMetric(c.confirmed, prometheus.GaugeValue, cc.c, id)
	}
}