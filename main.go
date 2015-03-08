package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/trotha01/kafka-viz/kafka"
	"golang.org/x/net/websocket"
)

type config struct {
	host           string
	port           string
	logDir         string
	logFile        string
	kafkaBinDir    string
	kafkaConfigDir string
	kafkaHost      string
	kafkaPort      string
}

var conf *config
var logger *log.Logger
var logfile *os.File

func init() {
	configFromEnv()
	initializeLogger()
}

// Echo the data received on the WebSocket.
func pollTopic(kafka *client.KafkaConfig) func(*websocket.Conn) {
	//TODO: close if previous go routine is running
	closeChan := make(chan struct{})
	openChan := make(chan struct{}, 1)

	return func(ws *websocket.Conn) {
		// close previous polling
		// ignore if there wasn't a previous polling
		select {
		case <-openChan:
			logger.Printf("New topic with old open chan. Insert into close chan")
			closeChan <- struct{}{}
		default:
			logger.Printf("No open chans yet for topic")
		}
		logger.Println("Insert into open chan")
		openChan <- struct{}{}

		// Get topic from websocket
		var topic string
		err := websocket.Message.Receive(ws, &topic)
		if err == io.EOF {
			logger.Println("websocket EOF")
			return
		}
		if err != nil {
			logger.Printf("Error reading from websocket: %s", err.Error())
			return
		}
		logger.Printf("Poll Topic %s", topic)

		// Poll that topic
		topicDataChan := make(chan string)
		go kafka.Poll(topic, topicDataChan, closeChan) // poll for current topic metadata

		// Send polling results back through websocket
		for {
			select {
			case topicData := <-topicDataChan:
				fmt.Printf("%+v\n", topicData)
				io.Copy(ws, strings.NewReader(topicData))
			case <-closeChan:
				logger.Printf("close chan case in poll topic %s", topic)
				return
			}
		}
	}
}

func main() {
	rtc := mux.NewRouter()

	kafka, err := client.NewKafka(conf.kafkaHost, conf.kafkaPort)
	if err != nil {
		logger.Printf("Error creating kafka connections: %s", err.Error())
		os.Exit(1)
	}
	defer kafka.Close()

	rtc.HandleFunc("/topics", topicDataHandler(kafka))                                  // get metadata
	rtc.Handle("/topics/{topic}/poll", websocket.Handler(pollTopic(kafka)))             // poll for topic metadata
	rtc.HandleFunc("/topics/{topic}/{partition}/{offsetRange}", consumerHandler(kafka)) // get specific data
	rtc.HandleFunc("/topics/{topic}", producerHandler(kafka))                           // insert data

	rtc.PathPrefix("/").Handler(http.FileServer(http.Dir("./web/kafka_viz")))

	bind := fmt.Sprintf("%s:%s", conf.host, conf.port)
	logger.Printf("Listening on %s...", bind)

	err = http.ListenAndServe(bind, rtc)
	if err != nil {
		logger.Printf("Error serving: %s", err.Error())
		cleanup()
		os.Exit(1)
	}
}

func configFromEnv() {
	conf = new(config)
	conf.host = os.Getenv("HOST")
	conf.port = os.Getenv("PORT")
	conf.logDir = os.Getenv("LOG_DIR")
	conf.logFile = os.Getenv("LOG_FILE")
	conf.kafkaHost = os.Getenv("KAFKA_HOST")
	conf.kafkaPort = os.Getenv("KAFKA_PORT")
	conf.kafkaBinDir = os.Getenv("KAFKA_BIN_DIR")
	conf.kafkaConfigDir = os.Getenv("KAFKA_CONFIG_DIR")

	// defaults
	if conf.host == "" {
		conf.host = "127.0.0.1"
	}
	if conf.port == "" {
		conf.port = "8090"
	}
	if conf.logDir == "" {
		conf.logDir = "."
	}
	if conf.logFile == "" {
		conf.logFile = "STDOUT"
	}
	if conf.kafkaHost == "" {
		conf.kafkaHost = "localhost"
	}
	if conf.kafkaPort == "" {
		conf.kafkaPort = "9092"
	}
}

func topicDataHandler(kafka *client.KafkaConfig) func(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("here\n")
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("here 2\n")
		r.ParseForm()
		topics := r.Form["topic"]

		metadataResponse, err := kafka.TopicDataResponse(topics)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(metadataResponse[:]))
	}
}

func producerHandler(kafka *client.KafkaConfig) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			logger.Printf("Insert Data Request")
			params := mux.Vars(r)
			topic := params["topic"]
			r.ParseForm()
			data := r.FormValue("data")
			logger.Printf("%+v", data)
			kafka.Produce(data, topic)
			w.WriteHeader(204)
		}
	}
}

func consumerHandler(kafka *client.KafkaConfig) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Printf("Consume Data Request")
		params := mux.Vars(r)
		topic := params["topic"]
		partitionStr := params["partition"]

		offsetRange := params["offsetRange"]
		logger.Printf("Topic: "+topic+" Partition: "+partitionStr, "OffsetRange: "+offsetRange)

		partition, err := strconv.Atoi(partitionStr)
		if err != nil {
			logger.Printf("Invalid partition: %s", partitionStr)
			logger.Printf("Invalid partition error: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		var offsetStart int
		var offsetLength int

		if strings.Contains(offsetRange, "-") {
			parsedRange := strings.Split(offsetRange, "-")
			offsetStart, err = strconv.Atoi(parsedRange[0])
			if err != nil {
				logger.Printf("Invalid offset start in range: %s", offsetRange)
				logger.Printf("Invalid offset start error: %s", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
			}

			offsetEnd, err := strconv.Atoi(parsedRange[1])
			if err != nil {
				logger.Printf("Invalid offset end in range: %s", offsetRange)
				logger.Printf("Invalid offset end error: %s", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
			}

			offsetLength = offsetEnd - offsetStart + 1
			if offsetLength < 0 {
				offsetLength = -offsetLength
			}
		} else {
			offsetLength = 1
			offsetStart, err = strconv.Atoi(offsetRange)
			if err != nil {
				logger.Printf("Invalid offset: %s", offsetRange)
				logger.Printf("Invalid offset error: %s", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
		}

		data, err := kafka.ConsumeOffsets(offsetStart, offsetLength, topic, partition)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		response, err := json.Marshal(data)
		if err != nil {
			logger.Printf("Error consuming from kafka. Err: %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(response))
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

func cleanup() {
	logfile.Close()
}
