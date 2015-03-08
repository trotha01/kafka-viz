package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/shopify/sarama"
	"golang.org/x/net/websocket"
)

var chttp = http.NewServeMux()

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
func pollTopic() func(*websocket.Conn) {
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

var kafka kafkaConfig

func main() {
	rtc := mux.NewRouter()

	rtc.HandleFunc("/topics/{topic}/{partition}/{offsetRange}", consumerHandler) // get data
	rtc.HandleFunc("/topics/{topic}", producerHandler)                           // insert data
	rtc.HandleFunc("/topics", topicDataHandler)                                  // get metadata
	rtc.Handle("/topics/{topic}/poll", websocket.Handler(pollTopic()))
	rtc.PathPrefix("/").Handler(http.FileServer(http.Dir("./web/kafka_viz")))

	bind := fmt.Sprintf("%s:%s", conf.host, conf.port)
	logger.Printf("Listening on %s...", bind)

	kafka = newKafka(conf)
	// kafka.Run()

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

type metadataResponse struct {
	Result []topicMetadata `json:"result"`
}

func topicDataHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	topics := r.Form["topic"]

	metadataResponse, err := topicDataResponse(topics)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(metadataResponse[:]))
}

func topicDataResponse(topics []string) ([]byte, error) {

	metadata, err := kafka.Metadata(topics)
	if err != nil {
		return nil, err
	}

	metadataResponse := metadataResponse{}
	metadataResponse.Result = metadata

	response, err := json.Marshal(metadataResponse)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func producerHandler(w http.ResponseWriter, r *http.Request) {
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

func consumerHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("Consume Data Request")
	params := mux.Vars(r)
	topic := params["topic"]
	partitionStr := params["partition"]

	offsetRange := params["offsetRange"]
	logger.Printf("Topic: "+topic+" Partition: "+partitionStr, "OffsetRange: "+offsetRange)

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		logger.Printf("Invalid partition: %d", partitionStr)
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

	data, err := kafka.consumeOffsets(offsetStart, offsetLength, topic, partition)
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

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	logger.Printf("Request /")
	chttp.ServeHTTP(w, r)
	/*
		if strings.Contains(r.URL.Path, "/") {
			chttp.ServeHTTP(w, r)
		} else {
			fmt.Fprintf(w, "HomeHandler")
		}
	*/
}

type kafkaConfig struct {
	binDir    string
	configDir string
	broker    *sarama.Broker
	client    *sarama.Client
}

func newKafka(conf *config) kafkaConfig {
	kc := kafkaConfig{}
	kc.binDir = conf.kafkaBinDir
	kc.configDir = conf.kafkaConfigDir
	kc.configDir = conf.kafkaConfigDir

	broker := conf.kafkaHost + ":" + conf.kafkaPort
	//zookeeper = 2181
	kc.broker = sarama.NewBroker(broker)
	err := kc.broker.Open(nil)
	if err != nil {
		logger.Printf("Error connecting to kafka broker: %s", err.Error())
		os.Exit(1)
	}

	kc.client, err = sarama.NewClient("client_id", []string{broker}, sarama.NewClientConfig())
	if err != nil {
		logger.Printf("Error creating kafka client: %s", err.Error())
		os.Exit(1)
	}

	return kc
}

var zkCmd *exec.Cmd
var kfCmd *exec.Cmd

func (kc kafkaConfig) Poll(topic string, topicDataChan chan string, closeChan chan struct{}) {
	fmt.Printf("Polling Topic: %s", topic)
	ticker := time.NewTicker(time.Millisecond * 1000)
	go func() {
		for _ = range ticker.C {
			metadataResponse, err := topicDataResponse([]string{topic})
			if err != nil {
				fmt.Printf("Error polling topic for metadata: %s", err.Error())
				return
			}

			topicDataChan <- string(metadataResponse[:])

			// Return if closeChan has an item
			select {
			case <-closeChan:
				return
			default:
			}
		}
	}()
	time.Sleep(time.Millisecond * 10000)
	ticker.Stop()
	fmt.Println("Ticker stopped")
}

func (kc kafkaConfig) Run() {
	/*
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

			time.Sleep(20 * time.Second)
	*/
	// kc.consumeOffsets(0, 10, "test", 0)
	// kc.Produce("from a function!", "test")
}

// Returns metadata about kafka
type topicMetadata struct {
	Name           string              `json:"name"`
	Partitions     int                 `json:"partitions"`
	Replication    int                 `json:"replication"`
	Partition_info []partitionMetadata `json:"partition_info"`
}

/*
   {
       "result": [
           { "name" : "topic1",
             "paritions" : 4,
             "replication" : 3,
             "partition_info" : [
                   {"length": 24},
                   {"length": 25},
                   {"length": 22},
                   {"length": 24}
             ]
           },
           { "name" : "topic2",
             "replication" : 5,
             "paritions" : 1,
             "partition_info" : [
                   {"length": 103}
             ]
           }
       ]
   }
*/

func (kc kafkaConfig) Metadata(topics []string) ([]topicMetadata, error) {
	request := sarama.MetadataRequest{}
	if len(topics) != 0 {
		request.Topics = topics
	}

	response, err := kc.broker.GetMetadata("myClient", &request)
	if err != nil {
		logger.Printf("Error getting metadata from broker: %s", err.Error())
		return nil, err
	}

	metadata := make([]topicMetadata, len(response.Topics))
	for i, topic := range response.Topics {
		metadata[i].Name = topic.Name
		metadata[i].Partitions = len(topic.Partitions)
		metadata[i].Partition_info = make([]partitionMetadata, len(topic.Partitions))
		replicationFactor, err := kc.TopicReplicationFactor(topic.Name, 0)
		if err != nil {
			logger.Printf("Error getting replication factor: %s", err.Error())
			return nil, err
		}
		metadata[i].Replication = replicationFactor
		for j, partition := range topic.Partitions {
			partitionInfo, err := kc.PartitionMetadata(topic.Name, partition.ID)
			if err != nil {
				logger.Printf("Error getting partition metadata: %s", err.Error())
				return nil, err
			}
			metadata[i].Partition_info[j] = *partitionInfo
		}
	}
	return metadata, nil
}

type partitionMetadata struct {
	Length int64 `json:"length"`
	Id     int32 `json:"id"`
}

// Sarama requires a partition?
func (kc kafkaConfig) TopicReplicationFactor(topic string, partition int32) (int, error) {
	replicaIDs, err := kc.client.Replicas(topic, partition)
	if err != nil {
		return -1, err
	}
	return len(replicaIDs), nil

}

func (kc kafkaConfig) PartitionMetadata(topic string, partition int32) (*partitionMetadata, error) {
	latestOffset, err := kc.client.GetOffset(topic, partition, sarama.LatestOffsets)
	if err != nil {
		logger.Printf("Error getting client offset: %s", err.Error())
		return nil, err
	}

	return &partitionMetadata{Length: latestOffset, Id: partition}, nil
}

func (kc kafkaConfig) Produce(message string, topic string) {
	producer, err := sarama.NewProducer(kc.client, nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message)}
}

type kafkaMessage struct {
	Offset  int64  `json:"offset"`
	Message string `json:"message"`
}

func (kc kafkaConfig) consumeOffsets(offset int, offsetCount int, topic string, partition int) ([]kafkaMessage, error) {
	master, err := sarama.NewConsumer(kc.client, nil)
	if err != nil {
		logger.Printf("Error creating sarama consumer: " + err.Error())
		return nil, err
	}

	config := sarama.NewPartitionConsumerConfig()
	config.OffsetMethod = sarama.OffsetMethodManual
	config.OffsetValue = int64(offset)
	consumer, err := master.ConsumePartition(topic, int32(partition), config)
	if err != nil {
		logger.Fatal("Error consuming partition: ", err.Error())
		return nil, err
	}

	result := make([]kafkaMessage, offsetCount)
	var value string
	for i := 0; i < offsetCount; i++ {
		select {
		case message := <-consumer.Messages():
			value = string(message.Value[:])
			result[i] = kafkaMessage{Message: value, Offset: message.Offset}
		case err := <-consumer.Errors():
			logger.Printf("Consumer error. Error: %s", err.Error())
			return nil, err
		}
	}
	return result, nil
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

	kc.broker.Close()
	kc.client.Close()
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
