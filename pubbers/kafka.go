package pubbers

import (
	"log"
	"os"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaWriter struct {
	Producer *kafka.Producer
	Topic string
}

func NewKafkaWriter() (KafkaWriter, error) {
	topic := os.Getenv("PUB_TOPIC")

	// TODO: Validate env vars or throw error

	brokers := os.Getenv("KAFKA_BROKERS")

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"message.send.max.retries": "50",
		"retry.backoff.ms": "5000",
		"partitioner": "murmur2_random", // consistent with java
	})

	return KafkaWriter{p, topic}, err
}

func (writer KafkaWriter) Publish(messages chan QueuedMessage, errs chan error, close bool) WriteResults {

	p := writer.Producer
	topic := writer.Topic

	sent := 0
	go func(){
		for msg := range messages {

			km := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key: msg.Key,
				Value: msg.Value}

			err := p.Produce(km, nil)
			if err != nil {
				errs <- err
				break
			}
			sent++
		}

		p.Flush(15 * 1000)

		if close {
			p.Close()
		}
	}()

	written := 0

	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				written++
			}

		case kafka.Error:
			e := ev
			if e.IsFatal() {
				errs <- e
				break
			} else {
				log.Printf("Non-fatal Error: %v\n", e)
			}
		}

		if written == sent {
			break
		}
	}

	return WriteResults{sent, written}
}
