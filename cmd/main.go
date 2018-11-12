package main

import (
	"fmt"
	"github.com/mqtt_mini_client"
	"time"
)

/* TODO: correct bug in client lib that crashes app if anything is done before connect */

func main() {
	var counter uint = 0

	client := mqtt_mini_client.NewMqttClient("JoeClient", "localhost:1883")
	client.OnConnect = func(c *mqtt_mini_client.MqttClient) {
		fmt.Printf("Doing some init\n")
		c.Subscribe("/demo")
		fmt.Printf("Done with init\n")
	}

	go func() {
		for {
			time.Sleep(3 * time.Second)
			client.Publish("/counter", []byte(fmt.Sprintf("%d", counter)))
			fmt.Printf("Sent counter %d\n", counter)
			counter++
		}
	}()

	client.Run()
}
