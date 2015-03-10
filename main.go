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
	"sync"

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
	rtc.HandleFunc("/topics/{topic}/{keyword}", searchHandler(kafka))                   // search data
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

func topicDataHandler(kafka *client.KafkaConfig) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
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

func searchHandler(kafka *client.KafkaConfig) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Printf("Search Topic Request")
		params := mux.Vars(r)
		topic := params["topic"]
		keyword := params["keyword"]
		logger.Printf("Searching topic %s for %s", topic, keyword)
		found := make(chan string)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			kafka.SearchTopic(found, topic, keyword)
		}()

		go func() {
			for {
				select {
				case match := <-found:
					fmt.Println("Match: ", match)
					fmt.Fprintf(w, match)
				default:
				}
			}
		}()

		wg.Wait()
		fmt.Printf("Done searching Topic")
		return

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

func offsetRangeFromString(offsetRange string) (int, int, error) {
	if strings.Contains(offsetRange, "-") {
		parsedRange := strings.Split(offsetRange, "-")
		offsetStart, err := strconv.Atoi(parsedRange[0])
		if err != nil {
			return -1, -1, err
		}

		offsetEnd, err := strconv.Atoi(parsedRange[1])
		if err != nil {
			return -1, -1, err
		}

		offsetLength := offsetEnd - offsetStart + 1
		if offsetLength < 0 {
			offsetLength = -offsetLength
		}
		return offsetStart, offsetLength, nil
	}

	offsetLength := 1
	offsetStart, err := strconv.Atoi(offsetRange)
	if err != nil {
		return -1, -1, err
	}
	return offsetStart, offsetLength, nil

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
			return
		}

		offsetStart, offsetLength, err := offsetRangeFromString(offsetRange)
		if err != nil {
			logger.Printf("Invalid partition range: %s", offsetRange)
			logger.Printf("Invalid partition error: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		data, err := kafka.ConsumeOffsets(offsetStart, offsetLength, topic, partition)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response, err := json.Marshal(data)
		if err != nil {
			logger.Printf("Error consuming from kafka. Err: %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, string(response))
	}
}

// Echo the data received on the WebSocket.
func pollTopic(kafka *client.KafkaConfig) func(*websocket.Conn) {
	closeChan := make(chan struct{})
	openChan := make(chan struct{}, 1)

	return func(ws *websocket.Conn) {
		// close previous polling
		// ignore if there wasn't a previous polling
		select {
		case <-openChan:
			logger.Printf("New polling request. Close previous polling")
			closeChan <- struct{}{}
		default:
		}
		openChan <- struct{}{}

		// Get topic from websocket
		var topic string
		err := websocket.Message.Receive(ws, &topic)
		if err == io.EOF {
			logger.Println("polling websocket EOF")
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
				io.Copy(ws, strings.NewReader(topicData))
			case <-closeChan:
				logger.Printf("polling stopped for topic %s", topic)
				return

			}
		}
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
