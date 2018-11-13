package main

import (
	"fmt"
	"github.com/omzlo/clog"
	"github.com/omzlo/gomqtt_mini_client"
	"os"
	"time"
)

/* TODO: correct bug in client lib that crashes app if anything is done before connect */

func main() {
	var counter uint = 0

	clog.SetLogLevel(clog.DEBUGXX)
	clog.AddWriter(clog.ColorTerminal)

	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <mqtt_url>\n", os.Args[0])
		return
	}

	client, err := gomqtt_mini_client.NewMqttClient("", os.Args[1])

	if err != nil {
		clog.Error("%s", err)
		return
	}

	if err = client.Connect(); err != nil {
		clog.Error("%s", err)
		return
	}

	client.Subscribe("/demo")
	for {
		time.Sleep(3 * time.Second)
		if err = client.Publish("/counter", []byte(fmt.Sprintf("%d", counter))); err != nil {
			clog.Warning("Failed to send counter %d", counter)
		} else {
			clog.Info("Successfully sent counter %d", counter)
			counter++
		}
	}

	fmt.Println("Done.")
}
