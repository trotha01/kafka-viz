package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"
)

var chttp = http.NewServeMux()

type config struct {
	host           string
	port           string
	logDir         string
	logFile        string
	kafkaBinDir    string
	kafkaConfigDir string
}

var conf *config
var logger *log.Logger
var logfile *os.File

func init() {
	configFromEnv()
	initializeLogger()
}

func main() {
	chttp.Handle("/", http.FileServer(http.Dir("./")))

	http.HandleFunc("/", HomeHandler) // homepage

	bind := fmt.Sprintf("%s:%s", conf.host, conf.port)
	logger.Printf("Listening on %s...", bind)

	kafka := newKafka(conf.kafkaBinDir, conf.kafkaConfigDir)
	kafka.Run()

	err := http.ListenAndServe(bind, nil)
	if err != nil {
		logger.Printf("Error serving: %s", err.Error())
		cleanup()
		kafka.clean()
		os.Exit(1)
	}
}

func configFromEnv() {
	conf = new(config)
	conf.host = os.Getenv("HOST")
	conf.port = os.Getenv("PORT")
	conf.logDir = os.Getenv("LOG_DIR")
	conf.logFile = os.Getenv("LOG_FILE")
	conf.kafkaBinDir = os.Getenv("KAFKA_BIN_DIR")
	conf.kafkaConfigDir = os.Getenv("KAFKA_CONFIG_DIR")
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("Request /")
	chttp.ServeHTTP(w, r)

	if r.Method == "POST" {
		r.ParseForm()
		logger.Println(r.Form)
	}
}

type kafkaConfig struct {
	binDir    string
	configDir string
}

func newKafka(binDir, configDir string) kafkaConfig {
	kc := kafkaConfig{}
	kc.binDir = binDir
	kc.configDir = configDir
	return kc
}

var zkCmd *exec.Cmd
var kfCmd *exec.Cmd

func (kc kafkaConfig) Run() {
	logger.Println("Running kafka")
	logger.Println(kc.binDir + "/zookeeper-server-start.sh " + kc.configDir + "/zookeeper.properties")
	logger.Println(kc.binDir + "/kafka-server-start.sh " + kc.configDir + "/server.properties")

	zkCmd = exec.Command("/bin/sh", "-c", kc.binDir+"/zookeeper-server-start.sh "+kc.configDir+"/zookeeper.properties")
	kfCmd = exec.Command("/bin/sh", "-c", kc.binDir+"/kafka-server-start.sh "+kc.configDir+"/server.properties")

	zkCmd.Stdout = os.Stdout
	kfCmd.Stdout = os.Stdout

	err := zkCmd.Start()
	if err != nil {
		logger.Printf("Error running Zookeeper: %s", err.Error())
	}

	time.Sleep(20 * time.Second)

	err = kfCmd.Start()
	if err != nil {
		logger.Printf("Error running Kafka: %s", err.Error())
	}
}

func initializeLogger() {
	var logWriter io.Writer
	var err error
	if conf.logFile == "STDOUT" {
		logWriter = os.Stdout
	} else {
		logWriter, err = os.OpenFile(conf.logDir+"/"+conf.logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening log file: %v", err)
		}
	}

	logger = log.New(logWriter, "", log.Ldate|log.Ltime|log.Lshortfile)

	if logger == nil {
		log.Printf("Could not create logger\n")
	}
}

func (kc kafkaConfig) clean() {
	zkCmd = exec.Command("/bin/sh", "-c", kc.binDir+"/zookeeper-server-stop.sh "+kc.configDir+"/zookeeper.properties")
	kfCmd = exec.Command("/bin/sh", "-c", kc.binDir+"/kafka-server-stop.sh "+kc.configDir+"/server.properties")

	err := kfCmd.Start()
	if err != nil {
		logger.Printf("Zookeeper exit: %s", err.Error())
	}
	err = zkCmd.Start()
	if err != nil {
		logger.Printf("Zookeeper exit: %s", err.Error())
	}
}

func cleanup() {
	logfile.Close()
}
