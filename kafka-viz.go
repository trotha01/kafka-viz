package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/shopify/sarama"
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

var kafka kafkaConfig

func main() {
	rtc := mux.NewRouter()
	chttp.Handle("/", http.FileServer(http.Dir("./web/kafka_viz")))

	rtc.HandleFunc("/", HomeHandler)                                             // homepage
	rtc.HandleFunc("/topics/{topic}/{partition}/{offsetRange}", consumerHandler) // get data
	rtc.HandleFunc("/topics/{topic}", producerHandler)                           // insert data

	bind := fmt.Sprintf("%s:%s", conf.host, conf.port)
	logger.Printf("Listening on %s...", bind)

	kafka = newKafka(conf.kafkaBinDir, conf.kafkaConfigDir)
	kafka.Run()

	err := http.ListenAndServe(bind, rtc)
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

func producerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		logger.Printf("Insert Data Request")
		// params := mux.Vars(r)
		// topic := params["topic"]
	}
}

func consumerHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("Consume Data Request")
	params := mux.Vars(r)
	topic := params["topic"]
	partitionStr := params["partition"]

	offsetRange := params["offsetRange"]
	logger.Printf("Topic: "+topic+" Partition: "+partitionStr, "OffsetRange: "+offsetRange)

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		// return user error
		logger.Printf("Invalid partition: %d", partitionStr)
		logger.Printf("Invalid partition error: %s", err.Error())
	}

	var offsetStart int
	var offsetLength int

	if strings.Contains(offsetRange, "-") {
		parsedRange := strings.Split(offsetRange, "-")
		offsetStart, err := strconv.Atoi(parsedRange[0])
		if err != nil {
			// return user error
			logger.Printf("Invalid offset start in range: %s", offsetRange)
			logger.Printf("Invalid offset start error: %s", err.Error())
		}

		offsetEnd, err := strconv.Atoi(parsedRange[1])
		if err != nil {
			// return user error
			logger.Printf("Invalid offset end in range: %s", offsetRange)
			logger.Printf("Invalid offset end error: %s", err.Error())
		}

		offsetLength = offsetEnd - offsetStart
		if offsetLength < 0 {
			offsetLength = -offsetLength
		}
	} else {
		offsetLength = 1
		offsetStart, err = strconv.Atoi(offsetRange)
		if err != nil {
			// return user error
			logger.Printf("Invalid offset: %s", offsetRange)
			logger.Printf("Invalid offset error: %s", err.Error())
		}
	}

	//TODO: return results to user
	kafka.consumeOffsets(offsetStart, offsetLength, topic, partition)

	/*
		if r.Method == "POST" {
			r.ParseForm()
			logger.Println(r.Form)
		}
	*/
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("Request /")
	/*
		chttp.ServeHTTP(w, r)
	*/

	if strings.Contains(r.URL.Path, "/") {
		chttp.ServeHTTP(w, r)
	} else {
		fmt.Fprintf(w, "HomeHandler")
	}
	/*
		if r.Method == "POST" {
			r.ParseForm()
			logger.Println(r.Form)
		}
	*/
}

type kafkaConfig struct {
	binDir    string
	configDir string
	broker    string
}

func newKafka(binDir, configDir string) kafkaConfig {
	kc := kafkaConfig{}
	kc.binDir = binDir
	kc.configDir = configDir
	kc.broker = "localhost:9092"
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

	/*
		err := zkCmd.Start()
		if err != nil {
			logger.Printf("Error running Zookeeper: %s", err.Error())
		}

		time.Sleep(20 * time.Second)

		err = kfCmd.Start()
		if err != nil {
			logger.Printf("Error running Kafka: %s", err.Error())
		}

		time.Sleep(20 * time.Second)
	*/
	kc.consumeOffsets(0, 10, "test", 0)
	// kc.Produce("from a function!", "test")
}

func (kc kafkaConfig) Produce(message string, topic string) {
	// TODO: host port passed in
	// client, err := sarama.NewClient("client_id", []string{"localhost:9092"}, sarama.NewClientConfig())
	client, err := sarama.NewClient("client_id", []string{kc.broker}, sarama.NewClientConfig())
	if err != nil {
		panic(err)
	}
	defer client.Close()

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message)}
}

func (kc kafkaConfig) consumeOffsets(offset int, offsetCount int, topic string, partition int) {
	broker := sarama.NewBroker(kc.broker)
	err := broker.Open(nil)
	if err != nil {
		logger.Printf("Error opening sarama broker. Error: %s", err.Error())
	}
	defer broker.Close()

	client, err := sarama.NewClient("client_id", []string{broker.Addr()}, nil)
	if err != nil {
		logger.Fatal(err.Error())
	}
	defer client.Close()

	master, err := sarama.NewConsumer(client, nil)
	if err != nil {
		logger.Fatal(err)
	}

	config := sarama.NewPartitionConsumerConfig()
	config.OffsetMethod = sarama.OffsetMethodManual
	config.OffsetValue = int64(offset)
	consumer, err := master.ConsumePartition(topic, int32(partition), config)
	if err != nil {
		logger.Fatal(err)
	}

	for i := 0; i < offsetCount; i++ {
		select {
		case message := <-consumer.Messages():
			logger.Printf("Message value: %v", string(message.Value[:]))
		case err := <-consumer.Errors():
			logger.Printf("Consumer error. Error: %s", err.Error())
		}
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
