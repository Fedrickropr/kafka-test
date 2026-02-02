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

var Topics []string = []string{"testTopic"}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cleanup(stop, Topics...)

	log.Printf("Started")

	var wg sync.WaitGroup

	createTopic(Topics...)

	// Register consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		consume()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		produce()
	}()

	<-ctx.Done()
	wg.Wait()
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
	log.Printf("producing")

	for topic := range topics {
		writer := kafka.Writer{
			Addr:     kafka.TCP("Localhost:9092"),
			Topic:    "first-data",
			Balancer: &kafka.LeastBytes{},
		}
	}

	defer writer.Close()

	if err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(fmt.Sprintf("Key")),
		Value: []byte(fmt.Sprintf("Value")),
	}); err != nil {
		log.Printf("error writing message %s", err.Error())
		return
	}

	log.Printf("wrote Message")
}

func consume() {
	log.Printf("consuming")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{Address},
		GroupID: "consumer-group-1", // Allows scaling multiple consumers
		Topic:   "first-data",
	})

	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("failed to read message:", err)
		}

		log.Printf("received: %s (Offset: %d)\n", string(m.Value), m.Offset)
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
