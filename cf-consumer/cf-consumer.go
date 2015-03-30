package main

import (
	"fmt"
	"log"
	"time"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"github.com/streadway/amqp"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
)

type Status struct {
	InstanceProcessed int
	InstanceIndex     int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
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

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// start the work
	status := Status{}
	status.InstanceIndex = appEnv.Index
	go func() {
		for d := range msgs {
			fmt.Printf("Received a message: %s\n", d.Body)
			d.Ack(false)
			time.Sleep(100 * time.Millisecond)
			status.InstanceProcessed++
		}
	}()

	m := martini.Classic()
	// render html templates from templates directory
	m.Use(render.Renderer())

	m.Get("/", func(r render.Render) {
		r.HTML(200, "status", status)
	})

	m.Run()
}
