package pubbers

import (
	"log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaWriterConfig struct {
	KafkaBrokers string
	Topic string
}

type KafkaWriter struct {
	Producer *kafka.Producer
	Topic string
}

func NewKafkaWriter(cnf KafkaWriterConfig) (KafkaWriter, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cnf.KafkaBrokers,
		"message.send.max.retries": "50",
		"retry.backoff.ms": "5000",
		"partitioner": "murmur2_random", // consistent with java
	})

	return KafkaWriter{p, cnf.Topic}, err
}


func (writer KafkaWriter) Publish(messages chan QueuedMessage, errs chan error) (chan *kafka.Message, *WriteResults) {
	p := writer.Producer
	topic := writer.Topic

	results := &WriteResults{0, 0}

	go func(){
		pchan := p.ProduceChannel()

		for msg := range messages {
			km := kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key: msg.Key,
				Value: msg.Value}

			pchan <- &km
			results.Sent++
		}

		log.Printf("Flushing outstanding messages\n")
		p.Flush(15 * 1000)
		p.Close()
	}()

	outch := make(chan *kafka.Message)

	go func(){
		defer close(outch)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					errs <- m.TopicPartition.Error
				} else {
					outch <- m
					results.Written++
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
		}
	}()

	return outch, results
}
