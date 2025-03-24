package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConfig holds the configuration for Kafka connection
type KafkaConfig struct {
	Broker       string `json:"broker"`
	Topic        string `json:"topic"`
	ClientID     string `json:"client_id"`
	MaxRetry     int    `json:"max_retry"`
	RetryBackoff int    `json:"retry_backoff_ms"`
	TimeoutMS    int    `json:"timeout_ms"`
}

func main() {
	// Define command line flags
	logFilePath := flag.String("file", "", "Path to HDFS log file to monitor")
	checkInterval := flag.Int("interval", 5, "Check interval in seconds")
	configFilePath := flag.String("config", "kafka_config.json", "Path to Kafka configuration file")
	flag.Parse()

	// Validate log file path
	if *logFilePath == "" {
		log.Fatal("Please provide a log file path using the -file flag")
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(*logFilePath)
	if err != nil {
		log.Fatalf("Failed to resolve absolute path: %v", err)
	}

	// Check if file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Fatalf("Log file does not exist: %s", absPath)
	}

	// Load Kafka configuration
	kafkaConfig, err := loadKafkaConfig(*configFilePath)
	if err != nil {
		log.Fatalf("Failed to load Kafka configuration: %v", err)
	}

	// Create Kafka producer
	producer, err := createKafkaProducer(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	fmt.Printf("Starting to monitor HDFS log file: %s\n", absPath)
	fmt.Printf("Checking for updates every %d seconds\n", *checkInterval)
	fmt.Printf("Sending log updates to Kafka topic: %s\n", kafkaConfig.Topic)

	// Get initial file info
	lastSize, lastModTime := getFileInfo(absPath)
	fmt.Printf("Initial file size: %d bytes, last modified: %s\n", lastSize, lastModTime)

	// Start a goroutine to handle message delivery reports
	deliveryChan := make(chan kafka.Event)
	go handleDeliveryReports(deliveryChan)

	// Monitor loop
	ticker := time.NewTicker(time.Duration(*checkInterval) * time.Second)
	defer ticker.Stop()

	topic := kafkaConfig.Topic
	for range ticker.C {
		currentSize, currentModTime := getFileInfo(absPath)

		if currentSize > lastSize {
			bytesAdded := currentSize - lastSize
			fmt.Printf("[%s] File updated: %d new bytes added (total size: %d bytes)\n",
				time.Now().Format("2006-01-02 15:04:05"), bytesAdded, currentSize)

			// Read and send the new content to Kafka
			newContent := readNewContent(absPath, lastSize, currentSize)
			if newContent != "" {
				sendToKafka(producer, topic, newContent, deliveryChan)
			}

			lastSize = currentSize
			lastModTime = currentModTime
		} else if currentModTime.After(lastModTime) {
			fmt.Printf("[%s] File modified but size unchanged (size: %d bytes)\n",
				time.Now().Format("2006-01-02 15:04:05"), currentSize)
			lastModTime = currentModTime
		}
	}
}

// getFileInfo returns the size and modification time of a file
func getFileInfo(filePath string) (int64, time.Time) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		log.Printf("Warning: Failed to get file info: %v", err)
		return 0, time.Time{}
	}
	return fileInfo.Size(), fileInfo.ModTime()
}

// readNewContent reads new content added to the file and returns it as a string
func readNewContent(filePath string, oldSize, newSize int64) string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Warning: Failed to open file: %v", err)
		return ""
	}
	defer file.Close()

	// Seek to the position of the old file size
	_, err = file.Seek(oldSize, 0)
	if err != nil {
		log.Printf("Warning: Failed to seek in file: %v", err)
		return ""
	}

	// Read the new content
	newContent := make([]byte, newSize-oldSize)
	_, err = file.Read(newContent)
	if err != nil {
		log.Printf("Warning: Failed to read new content: %v", err)
		return ""
	}

	fmt.Println("--- New content ---")
	fmt.Println(string(newContent))
	fmt.Println("------------------")

	return string(newContent)
}

// loadKafkaConfig loads the Kafka configuration from a JSON file
func loadKafkaConfig(configFilePath string) (*KafkaConfig, error) {
	file, err := os.Open(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	configBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config KafkaConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}

	// Set defaults if not specified
	if config.Topic == "" {
		config.Topic = "hdfslog"
	}
	if config.ClientID == "" {
		config.ClientID = "hdfs-log-monitor"
	}
	if config.MaxRetry == 0 {
		config.MaxRetry = 3
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 100
	}
	if config.TimeoutMS == 0 {
		config.TimeoutMS = 5000
	}

	fmt.Printf("Loaded Kafka broker address: %s\n", config.Broker)

	return &config, nil
}

// createKafkaProducer creates a new Kafka producer
func createKafkaProducer(config *KafkaConfig) (*kafka.Producer, error) {
	// Configure the producer
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": "192.168.100.98:9092",
		"client.id":         config.ClientID,
		"retries":           config.MaxRetry,
		"retry.backoff.ms":  config.RetryBackoff,
		"socket.timeout.ms": config.TimeoutMS,
		"acks":              "1", // Wait for leader acknowledgment
	}

	fmt.Printf("Configuring Kafka producer with broker: %s\n", config.Broker)

	// Create the producer
	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
	}

	return producer, nil
}

// handleDeliveryReports processes message delivery reports
func handleDeliveryReports(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message to Kafka: %v", ev.TopicPartition.Error)
			} else {
				log.Printf("Successfully delivered message to topic %s [partition %d] at offset %v",
					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		}
	}
}

// sendToKafka sends a message to Kafka
func sendToKafka(producer *kafka.Producer, topic string, message string, deliveryChan chan kafka.Event) {
	// Create a message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value:     []byte(message),
		Key:       []byte(fmt.Sprintf("hdfs-log-%d", time.Now().UnixNano())),
		Timestamp: time.Now(),
	}

	// Produce the message
	err := producer.Produce(msg, deliveryChan)
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
	}
}
