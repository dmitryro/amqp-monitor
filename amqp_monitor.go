package main

import (
	"encoding/json"
	"flag"
	"fmt"
        "strings"
        "time"
	"github.com/streadway/amqp"
	"log"
)

type Task struct {
    report_id string `json:"report_id"`
}

var (
	amqpURI = flag.String("amqp", "amqp://admin_user:admin_pass@127.0.0.1:5672/", "AMQP URI")
)



func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func init() {
	flag.Parse()
	initAmqp()
}

var conn *amqp.Connection
var ch *amqp.Channel
var replies <-chan amqp.Delivery

func initAmqp() {
	var err error
	var q amqp.Queue

	conn, err = amqp.Dial(*amqpURI)
	failOnError(err, "Failed to connect to RabbitMQ")

	log.Printf("got Connection, getting Channel...")

	ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	log.Printf("got Channel, declaring Exchange (%s)", "ourqueue")

	err = ch.ExchangeDeclare(
		"ourqueue", // name of the exchange
		"fanout",           // type
		false,               // durable
		false,              // delete when complete
		false,              // internal
		false,              // noWait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare the Exchange")

	log.Printf("declared Exchange, declaring Queue (%s)", "ourqueue")

	q, err = ch.QueueDeclare(
		"\"ourqueue\"", // name, leave empty to generate a unique name
	    true,            // durable
		false,           // delete when usused
		false,           // exclusive
		false,           // noWait
		nil,             // arguments
	)
	failOnError(err, "Error declaring the Queue")

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		q.Name, q.Messages, q.Consumers, "ourqueue")

	err = ch.QueueBind(
		q.Name,             // name of the queue
		"\"ourqueue\"",      // bindingKey
		"\"ourqueue\"", // sourceExchange
		false,              // noWait
		nil,                // arguments
	)
	failOnError(err, "Error binding to the Queue")

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", "ourqueue")

	replies, err = ch.Consume(
		q.Name,            // queue
		"ourqueue",        // consumer
		true,              // auto-ack
		false,             // exclusive
	        false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Error consuming the Queue")
}

func notifyFailed() {

}

func main() {
	log.Println("Start consuming the Queue...")
	var count int = 1


	for r := range replies {
        var s = string(r.Body)
        s = strings.TrimSuffix(s, "]")
        s = strings.TrimSuffix(s, "]")
        s = strings.TrimPrefix(s, "[")
        s = strings.TrimPrefix(s, "[")

        s = strings.Split(s, ",")[0]
        s = fmt.Sprintf("%s}",s)

        var data map[string]interface{}
        err := json.Unmarshal([]byte(s), &data)
        if err != nil {
            panic(err)
        }
        fid, _ := data["report_id"].(float64)
        id := int(fid)
        log.Printf("Consuming reply number %d for report ID %d", count, id)  
        time.Sleep(10 * time.Second)
        fmt.Printf("Time to mark the report ID %d as failed ...", id)
        count++
	}
}

