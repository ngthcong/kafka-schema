package main

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
)

type (
	Producer interface {
		Send(topic string, msg ProducerMessage) error
		Close() error
	}
	KafkaProducer struct {
		Prod sarama.SyncProducer
	}

	ProducerMessage interface {
		Key() string
	}
)

// NewProducer create new kafka producer with given brokers
func NewProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return &KafkaProducer{Prod: producer}, err
}

//Send send message to kafka server
func (k *KafkaProducer) Send(topic string, msg ProducerMessage) error {
	//Sending to kafka server
	jsonMsg, err := json.Marshal(msg.Key())
	if err != nil {
		return err
	}
	kafkaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(jsonMsg),
	}
	_, _, err = k.Prod.SendMessage(kafkaMsg)

	if err == nil {
		log.Printf("Send success Topic: %s || Message: %s \n", topic, msg.Key())
	}
	return err
}

//Close close kafka server
func (k *KafkaProducer) Close() error {
	return k.Prod.Close()
}
