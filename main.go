package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"io/ioutil"
	"log"
	"time"
)

type TxEventModel struct {
	Timestamp         time.Time   `json:"timestamp"`
	ReqUID            string      `json:"req_uid"`
	CompanyCode       string      `json:"company_code"`
	CompanyName       string      `json:"company_name"`
	Channel           string      `json:"channel"`
	Version           string      `json:"version"`
	Product           string      `json:"product"`
	EventCode         string      `json:"event_code"`
	EventName         string      `json:"event_name"`
	EventStatus       string      `json:"event_status"`
	ServiceName       string      `json:"service_name"`
	TransactionDetail interface{} `json:"transaction_detail"`
	Description       string      `json:"description"`
}
type TransactionDetail struct {
	TranAmount float64 `json:"tranAmount"`
	MerFeeAmt  float64 `json:"merFeeAmt"`
	CusFeeAmt  float64 `json:"cusFeeAmt"`
}

func main() {

	topic := "TxEvent"

	// 1) Create the producer
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	//avroSchema := `{"type":"record","name":"myrecord","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}`
	// 2) Fetch the latest version of the schema, or create a new one if it is the first

	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://0.0.0.0:8081")
	err = schemaRegistryClient.DeleteSubject(fmt.Sprintf("%s-value", topic), true)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error delete subject %s", err))
	}

	schema, err := schemaRegistryClient.GetLatestSchema(topic, false)
	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile("avro.avsc")
		schema, err = schemaRegistryClient.CreateSchema(topic, string(schemaBytes), srclient.Avro, false)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}
	log.Printf("Schema value, ID: %v || Schema: %v || Version: %v || Codec: %v \n", schema.ID(), schema.Schema(), schema.Version(), schema.Codec())
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	// 3) Serialize the record using the schema provided by the client,
	// making sure to include the schema id as part of the record.
	tranDetail := TransactionDetail{
		TranAmount: 200.5,
		MerFeeAmt:  100.4,
		CusFeeAmt:  23.5,
	}
	newComplexType := TxEventModel{
		Timestamp:         time.Now(),
		ReqUID:            "4324",
		CompanyCode:       "PH0013",
		CompanyName:       "ABC",
		Channel:           "BPP",
		Version:           "1",
		Product:           "PH001",
		EventCode:         "00",
		EventName:         "CONFIRM",
		EventStatus:       "SUCCESS",
		ServiceName:       "BILLPAYMENT",
		TransactionDetail: tranDetail,
		Description:       "",
	}
	value, _ := json.Marshal(newComplexType)
	native, _, err := schema.Codec().NativeFromTextual(value)
	if err != nil {
		panic(fmt.Sprintf("Convert byte to argo format failed: %s", err))
	}
	log.Printf("Native: %v \n", native)
	valueBytes, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		panic(fmt.Sprintf("Error BinaryFromNative failed: %s", err))
	}

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	key, _ := uuid.NewUUID()

	//Sending to kafka server

	kafkaMsg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Key:       sarama.StringEncoder(key.String()),
		Value:     sarama.StringEncoder(recordValue),
	}
	_, _, err = p.SendMessage(kafkaMsg)

	if err != nil {
		panic(fmt.Sprintf("Error send message %s", err))
	}
	log.Printf("Send success Topic: %s || Message: %v \n", topic, recordValue)
}
