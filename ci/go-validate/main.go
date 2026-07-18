// Constructs confluent-kafka-go clients from kafkaesq-converted configs.
//
// librdkafka rejects unknown properties and invalid values at construction,
// so a successful NewConsumer/NewProducer proves every key kafkaesq emitted
// is a real config the Go client accepts. No broker is contacted.
//
// Usage: go run . consumer.json producer.json
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func load(path string) *kafka.ConfigMap {
	data, err := os.ReadFile(path)
	die(err)
	var raw map[string]interface{}
	die(json.Unmarshal(data, &raw))
	cm := kafka.ConfigMap{}
	for key, value := range raw {
		// JSON numbers decode as float64; librdkafka wants ints.
		if f, ok := value.(float64); ok && f == float64(int(f)) {
			cm[key] = int(f)
		} else {
			cm[key] = value
		}
	}
	return &cm
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: go run . consumer.json producer.json")
		os.Exit(2)
	}

	consumerConf := load(os.Args[1])
	consumer, err := kafka.NewConsumer(consumerConf)
	die(err)
	die(consumer.Close())
	fmt.Printf("confluent-kafka-go consumer accepted %d configs\n", len(*consumerConf))

	producerConf := load(os.Args[2])
	producer, err := kafka.NewProducer(producerConf)
	die(err)
	producer.Close()
	fmt.Printf("confluent-kafka-go producer accepted %d configs\n", len(*producerConf))
}
