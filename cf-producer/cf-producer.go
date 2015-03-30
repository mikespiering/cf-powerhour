package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/go-martini/martini"
	"github.com/streadway/amqp"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func publishMessage(ch *amqp.Channel) {
	body := fmt.Sprintf("Received email from <%s>", randomdata.Email())
	err := ch.Publish(
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
}

func main() {
	appEnv, _ := cfenv.Current()
	services, err := appEnv.Services.WithTag("rabbitmq")
	failOnError(err, "Unable to find bound CloudAMQP instance")

	rabbitmq := services[0]
	rabbitmqURI := rabbitmq.Credentials["uri"]
	conn, err := amqp.Dial(rabbitmqURI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// start publishing
	go func() {
		for {
			publishMessage(ch)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	m := martini.Classic()
	m.Post("/publish/:amount", func(params martini.Params) string {
		x, err := strconv.Atoi(params["amount"])
		if err != nil {
			log.Printf("Unable to convert %s to int")
		}
		for i := 0; i < x; i++ {
			publishMessage(ch)
		}
		return "Published " + params["amount"] + " messages\n"
	})
	m.Get("/", func(params martini.Params) string {
		return "Messages are being published"
	})
	m.Run()

}
