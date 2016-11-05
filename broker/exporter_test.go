package broker_test

import (
	"testing"
	"github.com/buptmiao/msgo/broker"
)

func TestExporter_Collect(t *testing.T) {
	stat := broker.NewStat()
	stat.Add("msgo", 1)
	stat.Success("msgo", 1)
	stat.Subscribe("msgo")

	exporter := broker.NewExporter("msgo", "123")

	exporter.Scrape(stat.Get())
}

