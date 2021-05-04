package gomqtt_mini_client

import (
	"crypto/tls"
	"errors"
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

type MqttClientRequest struct {
	ControlPacketType MqttControlPacketType
	ResponseCallback  func(*MqttClient, error) error
	CreatedAt         time.Time
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
	terminationChannel   chan error
	ServerURL            *url.URL
	pendingRequests      map[uint16]*MqttClientRequest
	OnConnect            func(*MqttClient) error
	TLSConfig            *tls.Config
	dialCount            int
}

const (
	MQTT_CLIENT_CLOSED MqttClientState = iota
	MQTT_CLIENT_DIALED
	MQTT_CLIENT_CONNECTED
)

var (
	Terminate    = errors.New("MQTT client connection was terminated")
	TimeoutError = errors.New("Timed out while waiting for MQTT ack")
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
		terminationChannel:   make(chan error, 1),
		ServerURL:            u,
		pendingRequests:      make(map[uint16]*MqttClientRequest),
		OnConnect:            nil,
		TLSConfig:            &tls.Config{},
		dialCount:            0,
	}, nil
}

func (c *MqttClient) Connected() bool {
	return c.State == MQTT_CLIENT_CONNECTED
}

func (c *MqttClient) dial() error {
	var conn net.Conn
	var err error

	if c.ServerURL.Scheme == "mqtts" {
		conn, err = tls.Dial("tcp", c.ServerURL.Host, c.TLSConfig)
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

	c.dialCount++
	if c.dialCount == 1 {
		go c.processMessageLoop()
	}

	if c.OnConnect != nil {
		return c.OnConnect(c)
	}
	return nil
}

func (c *MqttClient) allocatePacketIdentifier() uint16 {
	if c.LastPacketIdentifier == 0 {
		c.LastPacketIdentifier = 1
	}
	pi := c.LastPacketIdentifier
	c.LastPacketIdentifier++
	return pi
}

func (c *MqttClient) SendAsync(msg *MqttMessage, cb func(*MqttClient, error) error) {

	switch msg.ControlPacketType {
	case SUBSCRIBE, UNSUBSCRIBE, PUBLISH:
		c.SendMutex.Lock()
		msg.Qos = 1
		pid := c.allocatePacketIdentifier()
		msg.VarHeader.AppendUint16(pid)

		c.pendingRequests[pid] = &MqttClientRequest{ControlPacketType: msg.ControlPacketType, ResponseCallback: cb, CreatedAt: time.Now()}
		c.SendMutex.Unlock()
	}
	if err := MqttMessageWrite(c.Conn, msg); err != nil {
		c.SendMutex.Lock()
		delete(c.pendingRequests, msg.GetPacketIdentifier())
		c.SendMutex.Unlock()
		cb(c, err)
	}
}

func (c *MqttClient) Send(msg *MqttMessage) error {
	responder := make(chan error, 1)
	c.SendAsync(msg, func(client *MqttClient, e error) error {
		responder <- e
		return nil
	})
	return <-responder
}

func (c *MqttClient) WaitTermination(duration time.Duration) error {
	if duration == 0 {
		err := <-c.terminationChannel
		if err != Terminate {
			return err
		}
		return nil
	}

	timeout := time.NewTimer(duration)

	select {
	case response := <-c.terminationChannel:
		if !timeout.Stop() {
			<-timeout.C
		}
		if response != Terminate {
			return response
		}
		return nil
	case <-timeout.C:
		return TimeoutError
	}
}

func (c *MqttClient) Terminate() error {
	c.RetryConnect = false
	return c.Close()
}

func (c *MqttClient) Close() error {

	c.State = MQTT_CLIENT_CLOSED
	c.Conn.Close()
	return c.Conn.Close()
}

func (c *MqttClient) Connect() error {
	return c.dial()
}

/*
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
*/

func (c *MqttClient) Subscribe(topic string) error {
	m := NewMqttMessage(SUBSCRIBE)
	m.Payload.AppendString(topic).AppendUint8(0)

	return c.Send(m)
}

func (c *MqttClient) Unsubscribe(topic string) error {
	m := NewMqttMessage(UNSUBSCRIBE)
	m.Payload.AppendString(topic)

	return c.Send(m)
}

func (c *MqttClient) Publish(topic string, value []byte) error {
	m := NewMqttMessage(PUBLISH)
	m.VarHeader.AppendString(topic)
	m.Payload.AppendBytes(value)

	return c.Send(m)
}

/********************************************
 * Connection management
 *
 */

func (c *MqttClient) processNextMessage() error {
	var msg *MqttMessage
	var err error

	for {
		err = c.Conn.SetReadDeadline(time.Now().Add(c.PingInterval))
		if err != nil {
			clog.Warning("MQTT SetReadDeadline failed: %s", err)
			return err
		}

		msg, err = MqttMessageRead(c.Conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				ping := NewMqttMessage(PINGREQ)
				if err := MqttMessageWrite(c.Conn, ping); err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			break
		}
	}

	switch msg.ControlPacketType {
	case PUBLISH:
		topic_name, packet_id := SplitString(msg.VarHeader.Data)
		if topic_name == nil {
			return fmt.Errorf("Received PUBLISH packet without topic")
		}
		if packet_id == nil {
			return fmt.Errorf("Received PUBLISH packet with missing packet id")
		}
		if msg.Qos == 1 {
			response := NewMqttMessage(PUBACK)
			response.VarHeader.AppendUint16(msg.GetPacketIdentifier())
			if err := MqttMessageWrite(c.Conn, response); err != nil {
				return err
			}
		}
		c.SubscribeCallback(string(topic_name), msg.Payload.Data)
		return nil
	case SUBACK, PUBACK, UNSUBACK:
		pid := msg.GetPacketIdentifier()
		c.SendMutex.Lock()
		pd := c.pendingRequests[pid]
		if pd != nil {
			delete(c.pendingRequests, pid)
		}
		c.SendMutex.Unlock()
		if pd == nil {
			return fmt.Errorf("Recieved message with unexepected packet identifier %d", pid)
		}
		if msg.ControlPacketType == SUBACK && pd.ControlPacketType != SUBSCRIBE {
			return fmt.Errorf("Recieved a SUBACK in response to a %s", pd.ControlPacketType)
		}
		if msg.ControlPacketType == UNSUBACK && pd.ControlPacketType != UNSUBSCRIBE {
			return fmt.Errorf("Recieved a UNSUBACK in response to a %s", pd.ControlPacketType)
		}
		if msg.ControlPacketType == PUBACK && pd.ControlPacketType != PUBLISH {
			return fmt.Errorf("Recieved a PUBACK in response to a %s", pd.ControlPacketType)
		}
		if err := pd.ResponseCallback(c, nil); err != nil {
			return err
		}

		pd = nil
		c.SendMutex.Lock()
		for cptype, pd2 := range c.pendingRequests {
			if time.Since(pd2.CreatedAt) > 3*time.Second {
				delete(c.pendingRequests, cptype)
				pd = pd2
				break
			}
		}
		c.SendMutex.Unlock()
		if pd != nil {
			if err := pd.ResponseCallback(c, TimeoutError); err != nil {
				return err
			}
		}
	case PINGRESP:
		/* Do nothing */
	default:
		return fmt.Errorf("Unexpected packet control type from server: %s", msg.ControlPacketType)
	}
	return nil
}

func (c *MqttClient) processMessageLoop() {
	var backoff time.Duration = 1
	var err error

	for {
		if c.State != MQTT_CLIENT_CONNECTED {
			err = c.dial()
			if err != nil {
				if !c.RetryConnect {
					break
				}
				Logger.Warning("Failed to connect to %s, waiting %d seconds to try again...", c.serverName(), backoff)
				time.Sleep(backoff * time.Second)
				if backoff < 16 {
					backoff *= 2
				}
				continue
			}
			backoff = 1
		}

		err := c.processNextMessage()
		if err != nil {
			c.Close()
			if err == Terminate {
				break
			}
			if !c.RetryConnect {
				break
			}
			continue
		}
	}
	c.terminationChannel <- err
}
