package main

import (
	"github.com/Shopify/sarama"

	"crypto/tls"
	"crypto/x509"
	"flag"
	span "github.com/yancl/hunter-tools/span"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic     = flag.String("topic", "jaeger-spans-test-001", "The Kafka topic")
	num       = flag.Int("num", 1, "Num of spans to generate")
	verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
	certFile  = flag.String("certificate", "", "The optional certificate file for client authentication")
	keyFile   = flag.String("key", "", "The optional key file for client authentication")
	caFile    = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySsl = flag.Bool("verify", false, "Optional verify ssl certificates chain")
)

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[span generator] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	producer := newAsyncProducer(brokerList)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close producer", err)
		}
	}()

	spanData, err := span.DumpSpans(span.CreateSpans(*num))
	if err != nil {
		log.Printf("dump spans err:%v", err)
	}

	if err := writeDataToKafka(producer, *topic, spanData); err != nil {
		log.Printf("write spans to kafka err:%v", err)
	}
}

func createTlsConfiguration() (t *tls.Config) {
	if *certFile != "" && *keyFile != "" && *caFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}

func newAsyncProducer(brokerList []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}

func writeDataToKafka(producer sarama.AsyncProducer, topic string, data []byte) error {
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	return nil
}
