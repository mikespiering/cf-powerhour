package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/streadway/amqp"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	appEnv, _ := cfenv.Current()
	services, _ := appEnv.Services.WithTag("myLemur")
	rabbitmq := services[0]
	username := rabbitmq.Credentials["username"]
	password := rabbitmq.Credentials["password"]
	hostname := rabbitmq.Credentials["hostname"]
	port := rabbitmq.Credentials["port"]
	path := rabbitmq.Credentials["path"]

	rabbitmqURI := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", username, password, hostname, port, path)

	conn, err := amqp.Dial(rabbitmqURI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	for {
		body := fmt.Sprintf("Received email from <%s>", randomdata.Email())
		err = ch.Publish(
			"",           // exchange
			"task_queue", // routing key
			false,        // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		failOnError(err, "Failed to publish a message")
		time.Sleep(50 * time.Millisecond)
	}
}
