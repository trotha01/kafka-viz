package client

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"sync"
	"time"

	"github.com/shopify/sarama"
)

type MetadataResponse struct {
	Result []TopicMetadata `json:"result"`
}

type KafkaConfig struct {
	// binDir    string
	// configDir string
	broker   *sarama.Broker
	client   *sarama.Client
	producer *sarama.Producer
}

func NewKafka(kafkaHost string, kafkaPort string) (*KafkaConfig, error) {
	kc := KafkaConfig{}
	// kc.binDir = conf.kafkaBinDir
	// kc.configDir = conf.kafkaConfigDir

	broker := kafkaHost + ":" + kafkaPort
	//zookeeper = 2181
	kc.broker = sarama.NewBroker(broker)
	err := kc.broker.Open(nil)
	if err != nil {
		return nil, err
	}

	kc.client, err = sarama.NewClient([]string{broker}, sarama.NewConfig())
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewProducerFromClient(kc.client)
	if err != nil {
		return nil, err
	}
	kc.producer = &producer

	return &kc, nil
}

/*
  Example Metadata() response
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
func (kc KafkaConfig) Metadata(topics []string) ([]TopicMetadata, error) {
	request := sarama.MetadataRequest{}
	if len(topics) != 0 {
		request.Topics = topics
	}

	response, err := kc.broker.GetMetadata(&request)
	if err != nil {
		return nil, err
	}

	metadata := make([]TopicMetadata, len(response.Topics))
	for i, topic := range response.Topics {
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
	return metadata, nil
}

type partitionMetadata struct {
	Length int64 `json:"length"`
	Id     int32 `json:"id"`
}

// Sarama requires a partition?
func (kc KafkaConfig) TopicReplicationFactor(topic string, partition int32) (int, error) {
	replicaIDs, err := kc.client.Replicas(topic, partition)
	if err != nil {
		return -1, err
	}
	return len(replicaIDs), nil

}

func (kc KafkaConfig) PartitionMetadata(topic string, partition int32) (*partitionMetadata, error) {
	latestOffset, err := kc.client.GetOffset(topic, partition, sarama.LatestOffsets)
	if err != nil {
		return nil, err
	}

	return &partitionMetadata{Length: latestOffset, Id: partition}, nil
}

func (kc KafkaConfig) Produce(message string, topic string) {
	(*kc.producer).Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message)}
}

type kafkaMessage struct {
	Offset  int64  `json:"offset"`
	Message string `json:"message"`
}

func (kc KafkaConfig) SearchTopic(found chan MessageMatch, stopSearch chan struct{}, topic string, keyword string) {
	topicMetadata, err := kc.Metadata([]string{topic})
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, partition := range topicMetadata[0].Partition_info {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			kc.SearchPartition(found, stopSearch, keyword, topic, partition)
		}(partition.Id)
	}

	wg.Wait()
}

type MessageMatch struct {
	Keyword   string `json:"keyword"`
	Message   string `json:"message"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (kc KafkaConfig) SearchPartition(found chan MessageMatch, stopSearch chan struct{}, keyword string, topic string, partition int32) {
	partitionData, err := kc.PartitionMetadata(topic, partition)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	master, err := sarama.NewConsumerFromClient(kc.client)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	consumer, err := master.ConsumePartition(topic, int32(partition), int64(0))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer consumer.Close()

	var i int64
	for i = 0; i < partitionData.Length-1; i++ {
		select {
		case message := <-consumer.Messages():
			match, err := regexp.Match(keyword, message.Value)
			if err != nil {
				fmt.Println(err.Error())
			}
			if match {
				found <- MessageMatch{
					Keyword:   keyword,
					Message:   string(message.Value[:]),
					Topic:     topic,
					Partition: partition,
					Offset:    message.Offset,
				}
			}
		case err = <-consumer.Errors():
			fmt.Println(err.Error())
			return
		case <-stopSearch:
			return
		}
	}
}

func (kc KafkaConfig) ConsumeOffsets(offset int, offsetCount int, topic string, partition int) ([]kafkaMessage, error) {

	master, err := sarama.NewConsumerFromClient(kc.client)
	if err != nil {
		return nil, err
	}

	consumer, err := master.ConsumePartition(topic, int32(partition), int64(offset))
	if err != nil {
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
			return nil, err
		}
	}
	return result, nil

}

// Returns metadata about kafka
type TopicMetadata struct {
	Name           string              `json:"name"`
	Partitions     int                 `json:"partitions"`
	Replication    int                 `json:"replication"`
	Partition_info []partitionMetadata `json:"partition_info"`
}

func (kc KafkaConfig) Close() {
	kc.broker.Close()
	kc.client.Close()
	(*kc.producer).Close()

	/*
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
	*/
}

func (kc KafkaConfig) Poll(topic string, topicDataChan chan string, closeChan chan struct{}) {
	ticker := time.NewTicker(time.Millisecond * 1000)
	go func() {
		for _ = range ticker.C {
			metadataResponse, err := kc.TopicDataResponse([]string{topic})
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
	time.Sleep(time.Millisecond * 2000)
	ticker.Stop()
	fmt.Println("Ticker stopped")
}

func (kc KafkaConfig) TopicDataResponse(topics []string) ([]byte, error) {
	metadata, err := kc.Metadata(topics)
	if err != nil {
		return nil, err
	}

	metadataResponse := MetadataResponse{}
	metadataResponse.Result = metadata

	response, err := json.Marshal(metadataResponse)
	if err != nil {
		return nil, err
	}

	return response, nil
}

var zkCmd *exec.Cmd
var kfCmd *exec.Cmd

func (kc KafkaConfig) Run() {
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
