package gomqtt_mini_client

import (
	"crypto/tls"
	"fmt"
	"github.com/omzlo/clog"
	"net"
	"net/url"
	"os"
	"sync"
	"time"
)

var Logger *clog.LogManager = clog.DefaultLogManager

type MqttClientState int

type MqttResponse struct {
	Message *MqttMessage
	Error   error
}

func CreateMqttResponseError(err error) MqttResponse {
	return MqttResponse{Message: nil, Error: err}
}

func CreateMqttResponse(msg *MqttMessage) MqttResponse {
	return MqttResponse{Message: msg, Error: nil}
}

type MqttClient struct {
	State                MqttClientState
	RetryConnect         bool
	ClientIdentifier     string
	Conn                 net.Conn
	SendMutex            sync.Mutex
	LastPacketIdentifier uint16
	SubscribeCallback    MqttCallback
	PingInterval         time.Duration
	Terminate            chan bool
	ServerURL            *url.URL
	RXQueue              chan MqttResponse
	Ticker               *time.Ticker
	OnConnect            func(*MqttClient)
}

const (
	MQTT_CLIENT_CLOSED MqttClientState = iota
	MQTT_CLIENT_DIALED
	MQTT_CLIENT_CONNECTED
)

type MqttCallback func(topic string, value []byte)

func (c *MqttClient) serverName() string {
	s := ""
	if c.ServerURL.User != nil {
		s += c.ServerURL.User.Username() + "@"
	}
	return s + c.ServerURL.Host
}

func (c *MqttClient) checkConnection() error {
	if c.State == MQTT_CLIENT_CONNECTED {
		return nil
	}
	return fmt.Errorf("Not connected to %s", c.serverName())
}

func defaultSubscribeCallback(topic string, value []byte) {
	Logger.Debug("Topic '%s' updated to %q\n", topic, value)
}

/****************************************
 * Main functions
 *
 */

func NewMqttClient(client_id string, server string) (*MqttClient, error) {
	u, err := url.Parse(server)

	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "":
		u.Scheme = "mqtt"
	case "mqtt", "mqtts":
		/* fall through */
	default:
		return nil, fmt.Errorf("Unrecognized scheme in URL for mqtt: %s", u.Scheme)
	}

	if u.Port() == "" {
		if u.Scheme == "mqtts" {
			u.Host += ":8883"
		} else {
			u.Host += ":1883"
		}
	}

	if client_id == "" {
		client_id = fmt.Sprintf("gomqtt_mini_client-%d", os.Getpid())
	}

	return &MqttClient{
		State:                MQTT_CLIENT_CLOSED,
		RetryConnect:         true,
		ClientIdentifier:     client_id,
		Conn:                 nil,
		LastPacketIdentifier: 0,
		SubscribeCallback:    defaultSubscribeCallback,
		PingInterval:         60 * time.Second,
		Terminate:            nil,
		ServerURL:            u,
		RXQueue:              nil,
		OnConnect:            nil,
	}, nil
}

func (c *MqttClient) Connected() bool {
	return c.State == MQTT_CLIENT_CONNECTED
}

func (c *MqttClient) Dial() error {
	var conn net.Conn
	var err error

	if c.State == MQTT_CLIENT_DIALED || c.State == MQTT_CLIENT_CONNECTED {
		return fmt.Errorf("MQTT Dial() called on network connection already established with %s.", c.serverName())
	}

	if c.ServerURL.Scheme == "mqtts" {
		conf := &tls.Config{
			// empty
		}
		conn, err = tls.Dial("tcp", c.ServerURL.Host, conf)
	} else {
		conn, err = net.Dial("tcp", c.ServerURL.Host)
	}

	c.Conn = conn
	return err
}

func (c *MqttClient) Connect() error {
	if c.State != MQTT_CLIENT_CONNECTED {
		if err := c.performConnect(); err != nil {
			return err
		}
	}
	c.Ticker = time.NewTicker(c.PingInterval)
	c.RXQueue = make(chan MqttResponse, 8)
	c.Terminate = make(chan bool)

	go c.readerRun()

	if c.OnConnect != nil {
		go c.OnConnect(c)
	}
	clog.Debug("Connect() completed")
	return nil
}

func (c *MqttClient) Disconnect() {
	c.RetryConnect = false
	if c.State == MQTT_CLIENT_CONNECTED {
		c.Terminate <- true
	}
}

func (c *MqttClient) SendMessage(msg *MqttMessage) error {
	c.SendMutex.Lock()
	defer c.SendMutex.Unlock()

	if c.State != MQTT_CLIENT_CONNECTED {
		return fmt.Errorf("Not connected to %s", c.serverName())
	}

	c.Ticker.Reset(c.PingInterval)
	return MqttMessageWrite(c.Conn, msg)
}

func (c *MqttClient) ReceiveMessage(packet_type MqttControlPacketType) (*MqttMessage, error) {
	response := <-c.RXQueue
	if response.Error != nil {
		return nil, response.Error
	}
	if response.Message.ControlPacketType != packet_type {
		return nil, fmt.Errorf("Unexpected packet type %s in response", packet_type.String())
	}
	return response.Message, response.Error
}

func (c *MqttClient) ReceiveMessageWithPacketId(packet_type MqttControlPacketType, packet_id uint16) (*MqttMessage, error) {
	m, e := c.ReceiveMessage(packet_type)
	if e != nil {
		return nil, e
	}
	if m.GetPacketIdentifier() != packet_id {
		return nil, fmt.Errorf("Mismatched packet identifier, expeted %d, got %d", packet_id, m.GetPacketIdentifier())
	}
	return m, nil
}

func (c *MqttClient) Subscribe(topic string) error {
	m := NewMqttMessage(SUBSCRIBE)
	m.Qos = 1

	pid := c.allocatePacketIdentifier()

	m.VarHeader.AppendUint16(pid)
	m.Payload.AppendString(topic).AppendUint8(0)

	if err := c.SendMessage(m); err != nil {
		return nil
	}

	_, err := c.ReceiveMessageWithPacketId(SUBACK, pid)
	if err != nil {
		return fmt.Errorf("Failed to subscribe to %s: %s", topic, err)
	}
	return nil
}

func (c *MqttClient) Unsubscribe(topic string) error {
	m := NewMqttMessage(UNSUBSCRIBE)
	m.Qos = 1

	pid := c.allocatePacketIdentifier()

	m.VarHeader.AppendUint16(pid)

	if err := c.SendMessage(m); err != nil {
		return nil
	}

	_, err := c.ReceiveMessageWithPacketId(UNSUBACK, pid)
	if err != nil {
		return fmt.Errorf("Failed to unsubscribe to %s: %s", topic, err)
	}
	return nil
}

func (c *MqttClient) Publish(topic string, value []byte) error {
	m := NewMqttMessage(PUBLISH)
	m.Qos = 1

	pid := c.allocatePacketIdentifier()

	m.VarHeader.AppendString(topic)
	m.VarHeader.AppendUint16(pid)
	m.Payload.AppendBytes(value)

	if err := c.SendMessage(m); err != nil {
		return nil
	}
	_, err := c.ReceiveMessageWithPacketId(PUBACK, pid)
	if err != nil {
		return fmt.Errorf("Failed to publish to %s: %s", topic, err)
	}
	return nil
}

func (c *MqttClient) RunEventLoop() error {
	return c.runEventLoop()
}

/********************************************
 * Connection management
 *
 */

func (c *MqttClient) readerRun() {
	Logger.DebugXX("Starting MQTT message reader loop")
	for c.processServerEvents() {
	}
	Logger.DebugXX("Leaving MQTT message reader loop")
}

func (c *MqttClient) closeConnection() {
	c.State = MQTT_CLIENT_CLOSED
	c.Conn.Close()
	c.Ticker.Stop()
}

func (c *MqttClient) runEventLoop() error {
	var backoff time.Duration = 2
	var err error

	for {
		for c.State != MQTT_CLIENT_CONNECTED {
			if err = c.Connect(); err != nil {
				if c.RetryConnect {
					Logger.Warning("Failed to connect to %s, waiting %d seconds to try again...", c.serverName(), backoff)
					time.Sleep(backoff * time.Second)
					if backoff < 16 {
						backoff *= 2
					}
				} else {
					return fmt.Errorf("Failed to connect to %s.", c.serverName())
				}
			} else {
				backoff = 2
			}
		}

		select {
		case <-c.Terminate:
			Logger.Info("Closing connection to %s due to termination signal.", c.ServerURL.Host)
			c.closeConnection()
			return fmt.Errorf("Disconnected")
		case <-c.Ticker.C:
			c.SendMessage(NewMqttMessage(PINGREQ))
		}
	}
}

func (c *MqttClient) performConnect() error {

	if c.State == MQTT_CLIENT_CONNECTED {
		return fmt.Errorf("MQTT Connect() already called on connection with %s.", c.serverName())
	}

	if c.State != MQTT_CLIENT_DIALED {
		if err := c.Dial(); err != nil {
			return err
		}
	}

	m := NewMqttMessage(CONNECT)

	m.Payload.AppendString(c.ClientIdentifier)

	m.VarHeader.AppendString("MQTT")
	m.VarHeader.AppendUint8(4)
	connectFlags := uint8(0)
	if c.ServerURL.User != nil {
		connectFlags |= 0x80 // USERNAME FLAG
		m.Payload.AppendString(c.ServerURL.User.Username())
		pass, ok := c.ServerURL.User.Password()
		if ok {
			connectFlags |= 0x40 // PASSWORD FLAG
			m.Payload.AppendString(pass)
		}
	}
	m.VarHeader.AppendUint8(connectFlags)
	m.VarHeader.AppendUint16(uint16(c.PingInterval.Seconds()))

	// We use the raw MqttMessageWrite/Read here
	if err := MqttMessageWrite(c.Conn, m); err != nil {
		return err
	}
	resp, err := MqttMessageRead(c.Conn)
	if err != nil {
		return err
	}
	if resp.ControlPacketType != CONNACK {
		return fmt.Errorf("Expected CONNACK from server, got %s instead", resp.ControlPacketType)
	}
	switch resp.VarHeader.Data[1] {
	case 0:
		c.State = MQTT_CLIENT_CONNECTED
		Logger.Info("Connected successfully to %s as %s", c.serverName(), c.ClientIdentifier)
		return nil
	case 1:
		return fmt.Errorf("CONNACK returned 0x01 Connection Refused, unacceptable protocol version")
	case 2:
		return fmt.Errorf("CONNACK returned 0x02 Connection Refused, identifier rejected")
	case 3:
		return fmt.Errorf("CONNACK returned 0x03 Connection Refused, Server unavailable")
	case 4:
		return fmt.Errorf("CONNACK returned 0x04 Connection Refused, bad user name or password")
	case 5:
		return fmt.Errorf("CONNACK returned 0x05 Connection Refused, not authorized")
	default:
		return fmt.Errorf("CONNACK returned unexpected status %02x", resp.VarHeader.Data[1])
	}
}

func (c *MqttClient) allocatePacketIdentifier() uint16 {
	pi := c.LastPacketIdentifier
	c.LastPacketIdentifier++
	return pi
}

func (c *MqttClient) processServerEvents() bool {
	for {
		msg, err := MqttMessageRead(c.Conn)
		if err != nil {
			c.RXQueue <- CreateMqttResponseError(err)
			return false
		}
		switch msg.ControlPacketType {
		case PUBLISH:
			topic_name, packet_id := SplitString(msg.VarHeader.Data)
			if topic_name == nil {
				c.RXQueue <- CreateMqttResponseError(fmt.Errorf("Received PUBLISH packet without topic"))
				return false
			}
			if packet_id == nil {
				c.RXQueue <- CreateMqttResponseError(fmt.Errorf("Received PUBLISH packet with missing packet id"))
				return false
			}
			if msg.Qos == 1 {
				response := NewMqttMessage(PUBACK)
				response.VarHeader.AppendUint16(msg.GetPacketIdentifier())
				if err := c.SendMessage(response); err != nil {
					c.RXQueue <- CreateMqttResponseError(err)
					return false
				}
			}
			c.SubscribeCallback(string(topic_name), msg.Payload.Data)
		case PINGRESP:
			/* Do nothing */
		default:
			c.RXQueue <- CreateMqttResponse(msg)
			return true
		}
	}
}
