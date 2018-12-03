package broker

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

// Exporter instance
type Exporter struct {
	namespace string
	addr      string
	sync.RWMutex
	uptime      *prometheus.GaugeVec
	totalMsgs   *prometheus.GaugeVec
	successMsgs *prometheus.GaugeVec

	topicMsgs        *prometheus.GaugeVec
	successTopicMsgs *prometheus.GaugeVec
	topicSubscribers *prometheus.GaugeVec
}

// NewExporter creates a new Exporter
func NewExporter(ns string, addr string) *Exporter {
	res := new(Exporter)
	res.namespace = ns
	res.addr = addr
	res.initGauges()
	return res
}

func (e *Exporter) initGauges() {
	e.uptime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "up_time",
		Help:      "The uptime till now, unit seconds",
	}, []string{"addr"})
	e.totalMsgs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "total_published_messages",
		Help:      "The total published messages till now",
	}, []string{"addr"})
	e.successMsgs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "total_consumed_messages",
		Help:      "The total consumed messages till now",
	}, []string{"addr"})
	e.topicMsgs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "total_published_messages_per_topic",
		Help:      "The total published messages per topic till now",
	}, []string{"addr", "topic"})
	e.successTopicMsgs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "total_consumed_messages_per_topic",
		Help:      "The total consumed messages per topic till now",
	}, []string{"addr", "topic"})
	e.topicSubscribers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace,
		Name:      "subscribers_number_per_topic",
		Help:      "The number of subscribers of per topic",
	}, []string{"addr", "topic"})
}

// Describe outputs the metric descriptions.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.uptime.Describe(ch)
	e.totalMsgs.Describe(ch)
	e.successMsgs.Describe(ch)
	e.topicMsgs.Describe(ch)
	e.successTopicMsgs.Describe(ch)
	e.topicSubscribers.Describe(ch)
}

// Collect fetches and updates the appropriate metrics.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.Lock()
	defer e.Unlock()

	stat := GetInstance().Stat()
	e.Scrape(stat)
	e.uptime.Collect(ch)
	e.totalMsgs.Collect(ch)
	e.successMsgs.Collect(ch)
	e.topicMsgs.Collect(ch)
	e.successTopicMsgs.Collect(ch)
	e.topicSubscribers.Collect(ch)
}

// Scrape set the metrics value to gauge
func (e *Exporter) Scrape(stat *Stat) {
	now := time.Now().UnixNano()
	elapsed := float64(now-stat.startTime) / float64(time.Second)
	e.uptime.WithLabelValues(e.addr).Set(elapsed)
	e.totalMsgs.WithLabelValues(e.addr).Set(float64(stat.totalMsg))
	e.successMsgs.WithLabelValues(e.addr).Set(float64(stat.successMsgs))
	for k, v := range stat.topicMsgs {
		e.topicMsgs.WithLabelValues(e.addr, k).Set(float64(v))
	}
	for k, v := range stat.successTopicMsgs {
		e.successTopicMsgs.WithLabelValues(e.addr, k).Set(float64(v))
	}
	for k, v := range stat.topicSubscribers {
		e.topicSubscribers.WithLabelValues(e.addr, k).Set(float64(v))
	}
}
