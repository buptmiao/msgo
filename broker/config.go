package broker

import (
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"math"
)

type Configure struct {
	HttpPort  int
	MsgPort   int

	Auth      bool
	UserName  string
	Token     string

	Retry     int

	Aof       string
	SyncType  int
	Threshold int
}

var Config *Configure = new(Configure)

func LoadConfig() {
	var configFile string
	// todo the default config Path
	flag.IntVar(&Config.HttpPort, "httpport", 13000, "the http port")
	flag.IntVar(&Config.MsgPort, "port", 13001, "the msg port")
	flag.IntVar(&Config.Retry, "r", 3, "the retry times")
	flag.StringVar(&Config.Aof, "aof", "msgo.aof", "the aof file path")
	flag.IntVar(&Config.SyncType, "sync", 0, "the default sync type of aof")
	flag.IntVar(&Config.Threshold, "rewrite-threshold", 10000, "the default threshold of deleteOps that triggers rewrite operation")
	flag.StringVar(&configFile, "c", "", "the config file path")
	flag.Parse()
	if configFile != "" {
		Bytes, err := ioutil.ReadFile(configFile)
		if err != nil {
			panic(fmt.Sprintf("reading config file error %s: %v", configFile, err))
		}
		if _, err := toml.Decode(string(Bytes), Config); err != nil {
			panic(fmt.Sprintf("parse config file error %s: %v", configFile, err))
		}
	}
	ArbitrateConfigs(Config)
}

func ArbitrateConfigs(c *Configure) {
	// check the ClusterName, ClusterName is used to Identify the clusters in the Local NetWork
	if c.HttpPort == c.MsgPort {
		panic("port conflict")
	}
	if c.HttpPort > math.MaxInt16 || c.HttpPort < 1024 {
		panic(fmt.Errorf("illegal http port %s", c.HttpPort))
	}

	if c.MsgPort > math.MaxInt16 || c.MsgPort < 1024 {
		panic(fmt.Errorf("illegal msg port %s", c.MsgPort))
	}

	if c.Retry > 10 {
		c.Retry = 10
	}
	if c.Retry < 1 {
		c.Retry = 1
	}
	if c.SyncType < 0 || c.SyncType > 2 {
		c.SyncType = 0
	}
	if c.Threshold < 1000 {
		c.Threshold = 1000
	}
	if c.Threshold > 1000000 {
		c.Threshold = 1000000
	}
}
