package main

import (
	"fmt"
	"log"
	"time"

	"github.com/go-martini/martini"
	"github.com/hoisie/redis"
	"github.com/martini-contrib/render"
	"github.com/streadway/amqp"

	cfenv "github.com/cloudfoundry-community/go-cfenv"
)

type Status struct {
	InstanceProcessed int
	InstanceIndex     int
	TotalProcessed    int64
	AMQP              bool
	REDIS             bool
}

func logOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func main() {
	status := Status{}
	appEnv, _ := cfenv.Current()
	status.InstanceIndex = appEnv.Index
	services, err := appEnv.Services.WithTag("rabbitmq")
	logOnError(err, "Unable to find bound CloudAMQP service")
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

		q, err := ch.QueueDeclare(
			"task_queue", // name
			true,         // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		logOnError(err, "Failed to declare a queue")

		err = ch.Qos(
			3,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		logOnError(err, "Failed to set QoS")

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		logOnError(err, "Failed to register a consumer")

		services, err = appEnv.Services.WithTag("redis")
		logOnError(err, "Unable to find bound CloudRedis service")
		var client redis.Client
		if err == nil {
			// setup redis connection if found
			status.REDIS = true
			services, _ := appEnv.Services.WithTag("redis")
			redis := services[0]
			client.Addr = redis.Credentials["hostname"] + ":" + redis.Credentials["port"]
			client.Password = redis.Credentials["password"]
		}

		// start the work
		go func() {
			for d := range msgs {
				fmt.Printf("Received a message: %s\n", d.Body)
				d.Ack(false)
				time.Sleep(100 * time.Millisecond)
				status.InstanceProcessed++
				if status.REDIS {
					status.TotalProcessed, _ = client.Incr("totalProcessed")
				}
			}
		}()
	}

	m := martini.Classic()
	m.Use(render.Renderer())

	m.Get("/", func(r render.Render) {
		r.HTML(200, "status", status)
	})

	m.Run()
}
