package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"
)

const Address string = "localhost:9092"

var topics []string = []string{"apples", "oranges", "bananas"}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cleanup(stop, topics...)

	log.Printf("Started")

	var wg sync.WaitGroup

	createTopic(topics...)

	// Register consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		consume(topics...)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		produce(topics...)
	}()

	wg.Wait()

	<-ctx.Done()
	log.Printf("Shutting down")
}

func createTopic(topics ...string) {
	// TODO create a topic
	conn, err := kafka.Dial("tcp", Address)
	if err != nil {
		log.Printf("could not dial Kafka")
		return
	}

	defer conn.Close()

	var topicList []kafka.TopicConfig
	for _, topic := range topics {
		topicList = append(topicList, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	err = conn.CreateTopics(topicList...)
	if err != nil {
		log.Printf("could not create topics \n %s", err.Error())
	}

	log.Printf("created Topics")
}

func produce(topics ...string) {
	log.Printf("[prod] producing")

	for _, topic := range topics {
		log.Printf("[prod] writing to topic %s", topic)

		writer := kafka.Writer{
			Addr:     kafka.TCP("Localhost:9092"),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}

		if err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(fmt.Sprintf("Key_topic_%s", topic)),
			Value: []byte(fmt.Sprintf("Value_topic_%s", topic)),
		}); err != nil {
			log.Printf("[prod] error writing message %s", err.Error())
			return
		}

		log.Printf("[prod] wrote Message to topic %s", topic)

		defer writer.Close()
	}

}

func consume(topics ...string) {
	log.Printf("[cons] consuming")

	for _, topic := range topics {
		log.Printf("[cons] reading from topic %s", topic)
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{Address},
			Topic:   topic,
		})

		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("[cons] failed to read message:", err)
			return
		}

		log.Printf("[cons] received: %s, %s \n", string(message.Key), string(message.Value))

		reader.Close()
	}
}

func cleanup(stop context.CancelFunc, topics ...string) {
	conn, err := kafka.Dial("tcp", Address)
	if err != nil {
		log.Printf("could not dial Kafka")
		return
	}

	defer conn.Close()

	conn.DeleteTopics(topics...)
	stop()
}
