package mqtt_mini_client

import (
	"fmt"
	"net"
	"time"
)

type MqttTransaction struct {
	ExpectedControlPacketType MqttControlPacketType
	PacketIdentifier          uint16
	TimeStamp                 time.Time
	Response                  chan *MqttMessage
}

type MqttClient struct {
	Connected         bool
	ClientIdentifier  string
	Conn              *net.TCPConn
	SubscribeCallback MqttCallback
	PingInterval      uint16
	Transactions      map[uint16]*MqttTransaction
	Terminate         chan bool
	LastTransaction   uint16
	ServerName        string
	ServerAddr        *net.TCPAddr
	TXQueue           chan *MqttMessage
	RXQueue           chan *MqttMessage
	Ticker            *time.Ticker
	OnConnect         func(*MqttClient)
}

type MqttCallback func(topic string, value []byte)

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
	close(transaction.Response)
	delete(c.Transactions, transaction.PacketIdentifier)
}

func defaultSubscribeCallback(topic string, value []byte) {
	fmt.Printf("Topic '%s' updated to %q\n", topic, value)
}

func NewMqttClient(client_id string, server string) *MqttClient {
	return &MqttClient{
		Connected:         false,
		ClientIdentifier:  client_id,
		Conn:              nil,
		SubscribeCallback: defaultSubscribeCallback,
		PingInterval:      60,
		Transactions:      nil,
		Terminate:         nil,
		LastTransaction:   0,
		ServerName:        server,
		ServerAddr:        nil,
		TXQueue:           nil,
		RXQueue:           nil,
	}
}

func (c *MqttClient) Connect() error {

	server_addr, err := net.ResolveTCPAddr("tcp", c.ServerName)

	if c.Connected {
		panic("Already connected")
	}

	if err != nil {
		return err
	}
	c.ServerAddr = server_addr

	if err = c.performConnect(); err != nil {
		return err
	}

	c.Terminate = make(chan bool)
	c.Transactions = make(map[uint16]*MqttTransaction)
	c.TXQueue = make(chan *MqttMessage, 8)
	c.RXQueue = make(chan *MqttMessage, 8)
	c.Connected = true

	go c.serverRun()
	c.Ticker = time.NewTicker(time.Duration(c.PingInterval) * time.Second)
	if c.OnConnect != nil {
		go c.OnConnect(c)
	}

	return nil
}

func (c *MqttClient) Disconnect() {
	c.TXQueue <- NewMqttMessage(DISCONNECT)
	c.Terminate <- true
}

func (c *MqttClient) Subscribe(topic string) error {
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

/****/
/*
func (c* MqttClient) processMessageToSend(m *MqttMessage) error {
  return MqttMessageWrite(c.Conn,m)
}
*/

func (c *MqttClient) performConnect() error {
	conn, err := net.DialTCP("tcp", nil, c.ServerAddr)
	if err != nil {
		return err
	}
	c.Conn = conn

	m := NewMqttMessage(CONNECT)
	m.VarHeader.AppendString("MQTT")
	m.VarHeader.AppendUint8(4)
	m.VarHeader.AppendUint8(0)
	m.VarHeader.AppendUint16(c.PingInterval)
	m.Payload.AppendString(c.ClientIdentifier)

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
		fmt.Printf("Connected OK\n")
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

func (c *MqttClient) serverRun() {
	c.Conn.SetLinger(0)

	conn := c.Conn
	for {
		m, err := MqttMessageRead(conn)
		if err != nil {
			fmt.Printf("Exiting read loop: %s\n", err)
			break
		}
		c.RXQueue <- m
	}
}

func (c *MqttClient) closeConnection() {
	c.Connected = false
	c.Conn.Close()
	c.Ticker.Stop()
}

func (c *MqttClient) Run() {
	var err error

	for {
		for !c.Connected {
			if err = c.Connect(); err != nil {
				time.Sleep(3)
			}
		}

		select {
		case m := <-c.TXQueue:
			//if c.Connected {
			err = MqttMessageWrite(c.Conn, m)
			//}
		case m := <-c.RXQueue:
			//if c.Connected {
			err = c.processMessageReceived(m)
			//} /* else silently drop packet */
		case <-c.Terminate:
			c.closeConnection()
			return
		case <-c.Ticker.C:
			c.TXQueue <- NewMqttMessage(PINGREQ)
			// TODO: scan transactions for idle ones, remove them / resend them
			// perhaps use a 1s ticker and then check ping time
			err = nil
		}
		if err != nil {
			c.closeConnection()
		}
	}
}
