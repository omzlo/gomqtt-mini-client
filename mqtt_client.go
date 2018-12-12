package gomqtt_mini_client

import (
	"crypto/tls"
	"fmt"
	"github.com/omzlo/clog"
	"net"
	"net/url"
	"os"
	"time"
)

var Logger *clog.LogManager = clog.DefaultLogManager

type MqttTransaction struct {
	ExpectedControlPacketType MqttControlPacketType
	PacketIdentifier          uint16
	TimeStamp                 time.Time
	Response                  chan *MqttMessage
}

type MqttClient struct {
	Connected         bool
	RetryConnect      bool
	ClientIdentifier  string
	Conn              net.Conn
	SubscribeCallback MqttCallback
	PingInterval      uint16
	Transactions      map[uint16]*MqttTransaction
	Terminate         chan bool
	LastTransaction   uint16
	ServerURL         *url.URL
	TXQueue           chan *MqttMessage
	RXQueue           chan *MqttMessage
	Ticker            *time.Ticker
	OnConnect         func(*MqttClient)
}

type MqttCallback func(topic string, value []byte)

func (c *MqttClient) serverName() string {
	s := ""
	if c.ServerURL.User != nil {
		s += c.ServerURL.User.Username() + "@"
	}
	return s + c.ServerURL.Host
}

func (c *MqttClient) checkConnection() error {
	if c.Connected {
		return nil
	}
	return fmt.Errorf("Not connected to %s", c.serverName())
}

/*****************************************
 * Transaction management
 *
 */

func (c *MqttClient) allocateTransaction(expected MqttControlPacketType) *MqttTransaction {
	c.LastTransaction++
	transaction := &MqttTransaction{
		ExpectedControlPacketType: expected,
		PacketIdentifier:          c.LastTransaction,
		TimeStamp:                 time.Now(),
		Response:                  make(chan *MqttMessage),
	}
	c.Transactions[c.LastTransaction] = transaction
	return transaction
}

func (c *MqttClient) findTransaction(packet_id uint16) *MqttTransaction {
	transaction, ok := c.Transactions[packet_id]
	if ok {
		return transaction
	}
	return nil
}

func (c *MqttClient) releaseTransaction(transaction *MqttTransaction) {
	delete(c.Transactions, transaction.PacketIdentifier)
	close(transaction.Response)
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
		Connected:         false,
		RetryConnect:      true,
		ClientIdentifier:  client_id,
		Conn:              nil,
		SubscribeCallback: defaultSubscribeCallback,
		PingInterval:      60,
		Transactions:      nil,
		Terminate:         nil,
		LastTransaction:   0,
		ServerURL:         u,
		TXQueue:           nil,
		RXQueue:           nil,
	}, nil
}

func (c *MqttClient) Connect() error {

	if c.Connected {
		panic("Already connected")
	}

	if err := c.performConnect(); err != nil {
		return err
	}

	go c.runEventLoop()

	return nil
}

func (c *MqttClient) Disconnect() {
	c.TXQueue <- NewMqttMessage(DISCONNECT)
	c.RetryConnect = false
	c.Terminate <- true
}

func (c *MqttClient) Subscribe(topic string) error {
	if err := c.checkConnection(); err != nil {
		return err
	}

	m := NewMqttMessage(SUBSCRIBE)
	m.Qos = 1

	transaction := c.allocateTransaction(SUBACK)
	defer c.releaseTransaction(transaction)

	m.VarHeader.AppendUint16(transaction.PacketIdentifier)
	m.Payload.AppendString(topic).AppendUint8(0)
	c.TXQueue <- m

	resp := <-transaction.Response

	if resp == nil {
		return fmt.Errorf("Failed to subscribe to %s", topic)
	}
	return nil
}

func (c *MqttClient) Unsubscribe(topic string) error {
	if err := c.checkConnection(); err != nil {
		return err
	}

	m := NewMqttMessage(UNSUBSCRIBE)
	m.Qos = 1

	transaction := c.allocateTransaction(UNSUBACK)
	defer c.releaseTransaction(transaction)

	m.VarHeader.AppendUint16(transaction.PacketIdentifier)
	c.TXQueue <- m

	resp := <-transaction.Response

	if resp == nil {
		return fmt.Errorf("Failed to unsubscribe to %s", topic)
	}
	return nil
}

func (c *MqttClient) Publish(topic string, value []byte) error {
	if err := c.checkConnection(); err != nil {
		return err
	}

	m := NewMqttMessage(PUBLISH)
	m.Qos = 1
	m.VarHeader.AppendString(topic)

	transaction := c.allocateTransaction(PUBACK)
	defer c.releaseTransaction(transaction)

	m.VarHeader.AppendUint16(transaction.PacketIdentifier)
	m.Payload.AppendBytes(value)
	c.TXQueue <- m

	resp := <-transaction.Response

	if resp == nil {
		return fmt.Errorf("Failed to publish to %s", topic)
	}
	return nil
}

/********************************************
 * Connection management
 *
 */

func (c *MqttClient) performConnect() error {
	var err error
	var conn net.Conn

	if c.ServerURL.Scheme == "mqtts" {
		conf := &tls.Config{
			// empty
		}
		conn, err = tls.Dial("tcp", c.ServerURL.Host, conf)
	} else {
		conn, err = net.Dial("tcp", c.ServerURL.Host)
	}

	if err != nil {
		return err
	}
	c.Conn = conn

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
	m.VarHeader.AppendUint16(c.PingInterval)

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
		Logger.Info("Connected successfully to %s", c.serverName())
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
		return fmt.Errorf("CONNACK returned %02x", resp.VarHeader.Data[1])
	}

}

func (c *MqttClient) processMessageReceived(m *MqttMessage) error {
	switch m.ControlPacketType {
	case PUBACK, SUBACK, UNSUBACK:
		transaction := c.findTransaction(m.GetPacketIdentifier())
		if transaction != nil {
			if transaction.ExpectedControlPacketType == m.ControlPacketType {
				transaction.Response <- m
			} else {
				transaction.Response <- nil
			}
		}
	case PINGRESP:
		/* do nothing */
	case PUBLISH:
		topic_name, packet_id := SplitString(m.VarHeader.Data)
		if topic_name == nil {
			return fmt.Errorf("Received PUBLISH packet without topic")
		}
		if packet_id == nil {
			return fmt.Errorf("Received PUBLISH packet withou missing packet id")
		}
		c.SubscribeCallback(string(topic_name), m.Payload.Data)
		if m.Qos == 1 {
			response := NewMqttMessage(PUBACK)
			response.VarHeader.AppendUint16(m.GetPacketIdentifier())
			c.TXQueue <- response
		}
	default:
		return fmt.Errorf("Unexpected packet %s from server", m.ControlPacketType)
	}
	return nil
}

func (c *MqttClient) readerRun() {
	Logger.DebugXX("Starting reader loop")
	conn := c.Conn
	for {
		m, err := MqttMessageRead(conn)
		if err != nil {
			Logger.Debug("Exiting read loop: %s", err)
			break
		}
		c.RXQueue <- m
	}
	c.Terminate <- true
}

func (c *MqttClient) closeConnection() {
	c.Connected = false
	for _, transaction := range c.Transactions {
		Logger.DebugXX("Transaction %d was cancelled while waiting for packet %s", transaction.PacketIdentifier, transaction.ExpectedControlPacketType)
		transaction.Response <- nil
	}
	c.Conn.Close()
	c.Ticker.Stop()
}

func (c *MqttClient) resetEventLoop() {
	c.Terminate = make(chan bool)
	c.Transactions = make(map[uint16]*MqttTransaction)
	c.TXQueue = make(chan *MqttMessage, 8)
	c.RXQueue = make(chan *MqttMessage, 8)

	go c.readerRun()

	c.Ticker = time.NewTicker(time.Duration(c.PingInterval) * time.Second)
	c.Connected = true
	if c.OnConnect != nil {
		go c.OnConnect(c)
	}
}

func (c *MqttClient) runEventLoop() {
	var err error
	var backoff time.Duration = 3

	c.resetEventLoop()

	for c.RetryConnect == true {
		for !c.Connected {
			if err = c.performConnect(); err != nil {
				Logger.DebugXX("Failed to connect to %s, waiting %d seconds to try again...", c.serverName(), backoff)
				time.Sleep(backoff * time.Second)
				if backoff < 60 {
					backoff *= 2
				}
			} else {
				backoff = 3
				c.resetEventLoop()
			}
		}

		select {
		case m := <-c.TXQueue:
			err = MqttMessageWrite(c.Conn, m)
		case m := <-c.RXQueue:
			err = c.processMessageReceived(m)
		case <-c.Terminate:
			err = fmt.Errorf("Received connection terminate request")
		case <-c.Ticker.C:
			c.TXQueue <- NewMqttMessage(PINGREQ)
			// TODO: scan transactions for idle ones, remove them / resend them
			// perhaps use a 1s ticker and then check ping time
			err = nil
		}
		if err != nil {
			Logger.Info("Closing connection to %s: %s", c.ServerURL.Host, err)
			c.closeConnection()
		}
	}
}
