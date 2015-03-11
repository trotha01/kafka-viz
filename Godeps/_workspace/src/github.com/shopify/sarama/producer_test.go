package sarama

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"testing"
)

const TestMessage = "ABC THE MESSAGE"

func closeProducer(t *testing.T, p Producer) {
	var wg sync.WaitGroup
	p.AsyncClose()

	wg.Add(2)
	go func() {
		for _ = range p.Successes() {
			t.Error("Unexpected message on Successes()")
		}
		wg.Done()
	}()
	go func() {
		for msg := range p.Errors() {
			t.Error(msg.Err)
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestSyncProducer(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	for i := 0; i < 10; i++ {
		leader.Returns(prodSuccess)
	}

	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		partition, offset, err := producer.SendMessage("my_topic", nil, StringEncoder(TestMessage))
		if partition != 0 {
			t.Error("Unexpected partition")
		}
		if offset != 0 {
			t.Error("Unexpected offset")
		}
		if err != nil {
			t.Error(err)
		}
	}

	safeClose(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestConcurrentSyncProducer(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	config := NewConfig()
	config.Producer.Flush.Messages = 100
	producer, err := NewSyncProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			partition, _, err := producer.SendMessage("my_topic", nil, StringEncoder(TestMessage))
			if partition != 0 {
				t.Error("Unexpected partition")
			}
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	safeClose(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestProducer(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	config := NewConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.AckSuccesses = true
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage), Metadata: i}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
			if msg.Metadata.(int) != i {
				t.Error("Message metadata did not match")
			}
		}
	}

	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestProducerMultipleFlushes(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)
	leader.Returns(prodSuccess)

	config := NewConfig()
	config.Producer.Flush.Messages = 5
	config.Producer.AckSuccesses = true
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for flush := 0; flush < 3; flush++ {
		for i := 0; i < 5; i++ {
			producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
		}
		for i := 0; i < 5; i++ {
			select {
			case msg := <-producer.Errors():
				t.Error(msg.Err)
				if msg.Msg.flags != 0 {
					t.Error("Message had flags set")
				}
			case msg := <-producer.Successes():
				if msg.flags != 0 {
					t.Error("Message had flags set")
				}
			}
		}
	}

	closeProducer(t, producer)
	leader.Close()
	seedBroker.Close()
}

func TestProducerMultipleBrokers(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader0 := newMockBroker(t, 2)
	leader1 := newMockBroker(t, 3)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader0.Addr(), leader0.BrokerID())
	metadataResponse.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader0.BrokerID(), nil, nil, ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, leader1.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodResponse0 := new(ProduceResponse)
	prodResponse0.AddTopicPartition("my_topic", 0, ErrNoError)
	leader0.Returns(prodResponse0)

	prodResponse1 := new(ProduceResponse)
	prodResponse1.AddTopicPartition("my_topic", 1, ErrNoError)
	leader1.Returns(prodResponse1)

	config := NewConfig()
	config.Producer.Flush.Messages = 5
	config.Producer.AckSuccesses = true
	config.Producer.Partitioner = NewRoundRobinPartitioner
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	closeProducer(t, producer)
	leader1.Close()
	leader0.Close()
	seedBroker.Close()
}

func TestProducerFailureRetry(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader1 := newMockBroker(t, 2)
	leader2 := newMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.AckSuccesses = true
	config.Producer.Retry.Backoff = 0
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader1.Returns(prodNotLeader)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, ErrNoError)
	leader1.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}
	leader1.Close()

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	leader2.Close()
	closeProducer(t, producer)
}

func TestProducerBrokerBounce(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)
	leaderAddr := leader.Addr()

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leaderAddr, leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.AckSuccesses = true
	config.Producer.Retry.Backoff = 0
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader.Close()                               // producer should get EOF
	leader = newMockBrokerAddr(t, 2, leaderAddr) // start it up again right away for giggles
	seedBroker.Returns(metadataResponse)         // tell it to go to broker 2 again

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}
	seedBroker.Close()
	leader.Close()

	closeProducer(t, producer)
}

func TestProducerBrokerBounceWithStaleMetadata(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader1 := newMockBroker(t, 2)
	leader2 := newMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.AckSuccesses = true
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 0
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader1.Close()                     // producer should get EOF
	seedBroker.Returns(metadataLeader1) // tell it to go to leader1 again even though it's still down
	seedBroker.Returns(metadataLeader1) // tell it to go to leader1 again even though it's still down

	// ok fine, tell it to go to leader2 finally
	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}
	seedBroker.Close()
	leader2.Close()

	closeProducer(t, producer)
}

func TestProducerMultipleRetries(t *testing.T) {
	seedBroker := newMockBroker(t, 1)
	leader1 := newMockBroker(t, 2)
	leader2 := newMockBroker(t, 3)

	metadataLeader1 := new(MetadataResponse)
	metadataLeader1.AddBroker(leader1.Addr(), leader1.BrokerID())
	metadataLeader1.AddTopicPartition("my_topic", 0, leader1.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader1)

	config := NewConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.AckSuccesses = true
	config.Producer.Retry.Max = 4
	config.Producer.Retry.Backoff = 0
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader1.Returns(prodNotLeader)

	metadataLeader2 := new(MetadataResponse)
	metadataLeader2.AddBroker(leader2.Addr(), leader2.BrokerID())
	metadataLeader2.AddTopicPartition("my_topic", 0, leader2.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataLeader2)
	leader2.Returns(prodNotLeader)
	seedBroker.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	seedBroker.Returns(metadataLeader1)
	leader1.Returns(prodNotLeader)
	seedBroker.Returns(metadataLeader2)

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}
	leader2.Returns(prodSuccess)
	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
			if msg.Msg.flags != 0 {
				t.Error("Message had flags set")
			}
		case msg := <-producer.Successes():
			if msg.flags != 0 {
				t.Error("Message had flags set")
			}
		}
	}

	seedBroker.Close()
	leader1.Close()
	leader2.Close()
	closeProducer(t, producer)
}

func TestProducerOutOfRetries(t *testing.T) {
	t.Skip("Enable once bug #294 is fixed.")

	seedBroker := newMockBroker(t, 1)
	leader := newMockBroker(t, 2)

	metadataResponse := new(MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := NewConfig()
	config.Producer.Flush.Messages = 10
	config.Producer.AckSuccesses = true
	config.Producer.Retry.Backoff = 0
	config.Producer.Retry.Max = 0
	producer, err := NewProducer([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodNotLeader := new(ProduceResponse)
	prodNotLeader.AddTopicPartition("my_topic", 0, ErrNotLeaderForPartition)
	leader.Returns(prodNotLeader)

	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			if msg.Err != ErrNotLeaderForPartition {
				t.Error(msg.Err)
			}
		case <-producer.Successes():
			t.Error("Unexpected success")
		}
	}

	seedBroker.Returns(metadataResponse)

	for i := 0; i < 10; i++ {
		producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder(TestMessage)}
	}

	prodSuccess := new(ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, ErrNoError)
	leader.Returns(prodSuccess)

	for i := 0; i < 10; i++ {
		select {
		case msg := <-producer.Errors():
			t.Error(msg.Err)
		case <-producer.Successes():
		}
	}

	leader.Close()
	seedBroker.Close()
	safeClose(t, producer)
}

// This example shows how to use the producer while simultaneously
// reading the Errors channel to know about any failures.
func ExampleProducer_select() {
	producer, err := NewProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &ProducerMessage{Topic: "my_topic", Key: nil, Value: StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

// This example shows how to use the producer with separate goroutines
// reading from the Successes and Errors channels. Note that in order
// for the Successes channel to be populated, you have to set
// config.Producer.AckSuccesses to true.
func ExampleProducer_goroutines() {
	config := NewConfig()
	config.Producer.AckSuccesses = true
	producer, err := NewProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                          sync.WaitGroup
		enqueued, successes, errors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _ = range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

ProducerLoop:
	for {
		message := &ProducerMessage{Topic: "my_topic", Value: StringEncoder("testing 123")}
		select {
		case producer.Input() <- message:
			enqueued++

		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}

// This example shows the basic usage pattern of the SyncProducer.
func ExampleSyncProducer() {
	producer, err := NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partition, offset, err := producer.SendMessage("my_topic", nil, StringEncoder("testing 123"))
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}
