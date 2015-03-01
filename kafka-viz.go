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

	rtc.HandleFunc("/topics/{topic}/{partition}/{offsetRange}", consumerHandler) // get data
	rtc.HandleFunc("/topics/{topic}", producerHandler)                           // insert data
	rtc.HandleFunc("/topics", topicDataHandler)                                  // get metadata
	rtc.PathPrefix("/").Handler(http.FileServer(http.Dir("./web/kafka_viz")))

	bind := fmt.Sprintf("%s:%s", conf.host, conf.port)
	logger.Printf("Listening on %s...", bind)

	kafka = newKafka(conf.kafkaBinDir, conf.kafkaConfigDir)
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
	conf.kafkaBinDir = os.Getenv("KAFKA_BIN_DIR")
	conf.kafkaConfigDir = os.Getenv("KAFKA_CONFIG_DIR")
}

type metadataResponse struct {
	Result []topicMetadata `json:"result"`
}

func topicDataHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	topics := r.Form["topic"]

	metadata, err := kafka.Metadata(topics)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	metadataResponse := metadataResponse{}
	metadataResponse.Result = metadata
	response, err := json.Marshal(metadataResponse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, string(response[:]))
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
	broker    string
}

func newKafka(binDir, configDir string) kafkaConfig {
	kc := kafkaConfig{}
	kc.binDir = binDir
	kc.configDir = configDir
	kc.broker = "localhost:9092"
	//zookeeper = 2181
	return kc
}

var zkCmd *exec.Cmd
var kfCmd *exec.Cmd

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
	broker := sarama.NewBroker(kc.broker)
	err := broker.Open(nil)
	if err != nil {
		return nil, err
	}
	defer broker.Close()

	// TODO: configurable topic
	request := sarama.MetadataRequest{}
	if len(topics) != 0 {
		request.Topics = topics
	}

	response, err := broker.GetMetadata("myClient", &request)
	if err != nil {
		return nil, err
	}

	fmt.Println("There are", len(response.Topics), "topics active in the cluster.")

	metadata := make([]topicMetadata, len(response.Topics))
	for i, topic := range response.Topics {
		fmt.Println("topic name: " + topic.Name)
		metadata[i].Name = topic.Name
		metadata[i].Partitions = len(topic.Partitions)
		metadata[i].Partition_info = make([]partitionMetadata, len(topic.Partitions))
		replicationFactor, err := kc.TopicReplicationFactor(topic.Name, 0)
		if err != nil {
			return nil, err
		}
		metadata[i].Replication = replicationFactor
		for j, partition := range topic.Partitions {
			partitionInfo, err := kc.PartitionMetadata(topic.Name, partition.ID)
			if err != nil {
				return nil, err
			}
			metadata[i].Partition_info[j] = *partitionInfo
		}
	}
	fmt.Println(metadata)
	return metadata, nil
}

type partitionMetadata struct {
	Length int64 `json:"length"`
}

// Sarama requires a partition?
func (kc kafkaConfig) TopicReplicationFactor(topic string, partition int32) (int, error) {
	client, err := sarama.NewClient("client_id", []string{kc.broker}, sarama.NewClientConfig())
	if err != nil {
		return -1, err
	}
	defer client.Close()

	replicaIDs, err := client.Replicas(topic, partition)
	if err != nil {
		return -1, err
	}
	return len(replicaIDs), nil

}

func (kc kafkaConfig) PartitionMetadata(topic string, partition int32) (*partitionMetadata, error) {
	client, err := sarama.NewClient("client_id", []string{kc.broker}, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}
	defer client.Close()

	latestOffset, err := client.GetOffset(topic, partition, sarama.LatestOffsets)
	if err != nil {
		return nil, err
	}
	logger.Printf("Topic: %s, Partition: %d", topic, partition)
	logger.Printf("LatestOffset: %d", latestOffset)

	return &partitionMetadata{Length: latestOffset}, nil
}

func (kc kafkaConfig) Produce(message string, topic string) {
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

func (kc kafkaConfig) consumeOffsets(offset int, offsetCount int, topic string, partition int) ([]string, error) {
	broker := sarama.NewBroker(kc.broker)
	err := broker.Open(nil)
	if err != nil {
		logger.Printf("Error opening sarama broker. Error: %s", err.Error())
		return nil, err
	}
	defer broker.Close()

	client, err := sarama.NewClient("client_id", []string{broker.Addr()}, nil)
	if err != nil {
		logger.Printf("Error creating sarama client: " + err.Error())
		return nil, err
	}
	defer client.Close()

	master, err := sarama.NewConsumer(client, nil)
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

	result := make([]string, offsetCount)
	var value string
	for i := 0; i < offsetCount; i++ {
		select {
		case message := <-consumer.Messages():
			value = string(message.Value[:])
			result[i] = value
		case err := <-consumer.Errors():
			logger.Printf("Consumer error. Error: %s", err.Error())
			return nil, err
		}
	}
	return result, nil
}

func (kc kafkaConfig) addTopic(topic string) error {
	broker := sarama.NewBroker(kc.broker)
	err := broker.Open(nil)
	if err != nil {
		logger.Printf("Error opening sarama broker. Error: %s", err.Error())
		return err
	}
	defer broker.Close()

	client, err := sarama.NewClient("client_id", []string{broker.Addr()}, nil)
	if err != nil {
		logger.Printf("Error creating sarama client: " + err.Error())
		return err
	}
	defer client.Close()

	return nil
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
