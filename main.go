package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const Address string = "localhost:9092"

var topics []string = []string{"apples", "oranges", "bananas"}

func main() {
	batchSize := flag.Int("batch", 1, "Number of messages per batch")
	workers := flag.Int("workers", 1, "Number of consumers to start")
	flag.Parse()

	log.Printf("Using batch size %d worker count %d", *batchSize, *workers)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cleanup(stop, topics...) // will also clean up topics

	log.Printf("Started")

	var wg sync.WaitGroup

	// set numPartitions to number of workers, so everyone has their own partition
	createTopic(*workers, topics...)

	prodChan := make(chan ProduceRecord, len(topics))
	consChan := make(chan ConsumeRecord, len(topics)**workers**batchSize)

	for i := 0; i < *workers; i++ {
		for _, topic := range topics {
			wg.Add(1)
			go func(id int, topic string) {
				defer wg.Done()
				consume(ctx, consChan, id, *batchSize, topic)
			}(i, topic)
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		produce(ctx, prodChan, *batchSize, topics...)
	}()

	var resWg sync.WaitGroup
	var prodResults []ProduceRecord
	var consResults []ConsumeRecord
	resWg.Add(1)
	go func() {
		defer resWg.Done()
		for r := range prodChan {
			prodResults = append(prodResults, r)
		}
	}()
	resWg.Add(1)
	go func() {
		defer resWg.Done()
		for r := range consChan {
			consResults = append(consResults, r)
		}
	}()

	wg.Wait()
	close(prodChan)
	close(consChan)

	resWg.Wait()

	log.Printf("Got producer results: %d", len(prodResults))
	log.Printf("Got producer results: %d", len(consResults))

	<-ctx.Done()
	log.Println("Shutting down")
}

func createTopic(numPartitions int, topics ...string) {
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
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		})
	}

	err = conn.CreateTopics(topicList...)
	if err != nil {
		log.Printf("could not create topics \n %s", err.Error())
	}

	log.Printf("created Topics")
}

type ProduceRecord struct {
	time  int64
	topic string
	keys  []string
}

func produce(ctx context.Context, res chan<- ProduceRecord, batchSize int, topics ...string) {
	log.Printf("[prod] producing")

	for _, topic := range topics {
		log.Printf("[prod] writing to topic %s", topic)

		writer := kafka.Writer{
			Addr:     kafka.TCP(Address),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}

		var messages []kafka.Message
		var keys []string
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("Key_%s_%d", topic, i)
			messages = append(messages, kafka.Message{
				Key:   []byte(key),
				Value: []byte(fmt.Sprintf("Value_topic_%s_%d", topic, i)),
			})

			keys = append(keys, key)
		}

		if err := writer.WriteMessages(ctx, messages...); err != nil {
			log.Printf("[prod] error writing message %s", err.Error())
			return
		}

		res <- ProduceRecord{time.Now().UnixMilli(), topic, keys}

		log.Printf("[prod] wrote messages to topic %s", topic)

		writer.Close()
	}
}

type ConsumeRecord struct {
	time   int64
	topic  string
	worker int
	key    string
}

func consume(ctx context.Context, res chan<- ConsumeRecord, workerId int, batchSize int, topic string) {
	log.Printf("[cons_%d] consuming", workerId)

	log.Printf("[cons_%d] reading from topic %s", workerId, topic)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{Address},
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
	})

	for i := 0; i < batchSize; i++ {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatal(fmt.Sprintf("[cons_%d] failed to read message:", workerId), err)
			return
		}

		res <- ConsumeRecord{time.Now().UnixMilli(), topic, workerId, string(message.Key)}
		log.Printf("[cons_%d] received: %s, %s \n", workerId, string(message.Key), string(message.Value))
	}

	reader.Close()
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
