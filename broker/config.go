package msgo

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
)

type config struct {
	HttpAddr string
	MsgAddr  string
	Auth     bool
	UserName string
	Token    string
	//Cluster name identifies your cluster for auto-discovery. If you're running
	//multiple clusters on the same network, make sure you're using unique names.
	//
	Retry int
	ClusterName string
}

var Config *config = new(config)

func LoadConfig() {
	var configFile string
	// todo the default config Path
	flag.StringVar(&configFile, "c", "", "the config file path")
	flag.Parse()
	Bytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("reading config file error %s: %v", configFile, err))
	}
	if _, err := toml.Decode(string(Bytes), Config); err != nil {
		panic(fmt.Sprintf("parse config file error %s: %v", configFile, err))
	}

	ArbitrateConfigs(Config)
}

func ArbitrateConfigs(c *config) {
	// check the ClusterName, ClusterName is used to Identify the clusters in the Local NetWork
	if c.ClusterName == "" {
		Error.Println("ClusterName should not be empty! please check you config file!")
		os.Exit(1)
	}

	if len(c.HttpAddr) == 0 {
		c.HttpAddr = ":12002"
	}

	Log.Println("Load config file success!")
}
