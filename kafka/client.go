package client

import (
	"fmt"

	"github.com/shopify/sarama"
)

var kafka kafkaConfig

type metadataResponse struct {
	Result []topicMetadata `json:"result"`
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

func (kc kafkaConfig) Metadata() ([]topicMetadata, error) {
	broker := sarama.NewBroker(kc.broker)
	err := broker.Open(nil)
	if err != nil {
		return nil, err
	}
	defer broker.Close()

	// TODO: configurable topic
	request := sarama.MetadataRequest{Topics: []string{"test"}}
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
		return nil, err
	}
	defer broker.Close()

	client, err := sarama.NewClient("client_id", []string{broker.Addr()}, nil)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	master, err := sarama.NewConsumer(client, nil)
	if err != nil {
		return nil, err
	}

	config := sarama.NewPartitionConsumerConfig()
	config.OffsetMethod = sarama.OffsetMethodManual
	config.OffsetValue = int64(offset)
	consumer, err := master.ConsumePartition(topic, int32(partition), config)
	if err != nil {
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
			return nil, err
		}
	}
	return result, nil
}

// Returns metadata about kafka
type topicMetadata struct {
	Name           string              `json:"name"`
	Partitions     int                 `json:"partitions"`
	Replication    int                 `json:"replication"`
	Partition_info []partitionMetadata `json:"partition_info"`
}
