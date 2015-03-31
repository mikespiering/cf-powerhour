package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"
	"github.com/streadway/amqp"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
)

var ch *amqp.Channel

type Status struct {
	InstanceProcessed int
	InstanceIndex     int
	AMQP              bool
}

func logOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func publishMessage(ch *amqp.Channel, status *Status) {
	if ch == nil {
		log.Printf("Can not publish to nil amqp.Channel")
		return
	}
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
	logOnError(err, "Failed to publish a message")
	status.InstanceProcessed++
}

func main() {
	status := Status{}
	appEnv, _ := cfenv.Current()
	status.InstanceIndex = appEnv.Index
	services, err := appEnv.Services.WithTag("rabbitmq")
	logOnError(err, "Unable to find bound CloudAMQP instance")
	if err == nil {
		// enable rabbitmq communication
		status.AMQP = true
		rabbitmq := services[0]
		rabbitmqURI := rabbitmq.Credentials["uri"]
		conn, err := amqp.Dial(rabbitmqURI)
		logOnError(err, "Failed to connect to RabbitMQ")
		defer conn.Close()

		ch, err := conn.Channel()
		logOnError(err, "Failed to open a channel")
		defer ch.Close()

		// start publishing
		go func() {
			for {
				publishMessage(ch, &status)
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

	m := martini.Classic()
	m.Use(render.Renderer())

	m.Post("/publish/:amount", func(params martini.Params) string {
		if !status.AMQP {
			log.Panicf("Not bound to CloudAMQP instance")
		}
		x, err := strconv.Atoi(params["amount"])
		if err != nil {
			log.Panicf("Unable to convert %s to int")
		}
		for i := 0; i < x; i++ {
			publishMessage(ch, &status)
		}
		return "Published " + params["amount"] + " messages\n"
	})

	m.Get("/kill", func() {
		os.Exit(1)
	})

	m.Get("/", func(r render.Render) {
		r.HTML(200, "status", status)
	})

	m.Run()
}
