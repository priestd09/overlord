package main

import (
	"flag"
	"overlord/lib/etcd"
	"overlord/lib/log"
	"overlord/mesos"
	"time"

	"github.com/BurntSushi/toml"
)

var confPath string
var defConf = &mesos.Config{
	User:        "root",
	Name:        "test",
	Master:      "127.0.0.1:5050",
	ExecutorURL: "http://127.0.0.1:8000/executor",
	DBEndPoint:  "http://127.0.0.1:2379",
	Checkpoint:  true,
	FailOver:    mesos.Duration(time.Hour),
}

func main() {
	log.Init(log.NewStdHandler())
	flag.StringVar(&confPath, "conf", "", "scheduler conf")
	flag.Parse()
	conf := new(mesos.Config)
	if confPath != "" {
		_, err := toml.DecodeFile(confPath, &conf)
		if err != nil {
			panic(err)
		}
	} else {
		conf = defConf
	}
	db, _ := etcd.New(conf.DBEndPoint)
	sched := mesos.NewScheduler(conf, db)
	sched.Run()
}