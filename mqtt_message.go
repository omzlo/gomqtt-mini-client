package gomqtt_mini_client

import (
	"errors"
	"fmt"
	"io"
)

type Buffer struct {
	Data []byte
}

func NewBuffer() *Buffer {
	return &Buffer{Data: make([]byte, 0, 8)}
}

func NewBufferBytes(b []byte) *Buffer {
	r := NewBuffer()
	r.Data = make([]byte, len(b))
	copy(r.Data, b)
	return r
}

func (b *Buffer) Length() uint {
	return uint(len(b.Data))
}

func (b *Buffer) AppendUint8(u uint8) *Buffer {
	b.Data = append(b.Data, byte(u))
	return b
}

func (b *Buffer) AppendUint16(u uint16) *Buffer {
	b.Data = append(b.Data, byte(u>>8), byte(u))
	return b
}

func (b *Buffer) AppendUint32(u uint32) *Buffer {
	b.Data = append(b.Data, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
	return b
}

func (b *Buffer) AppendBytes(bytes []byte) *Buffer {
	b.Data = append(b.Data, bytes...)
	return b
}

func (b *Buffer) AppendString(s string) *Buffer {
	return b.AppendUint16(uint16(len(s))).AppendBytes([]byte(s))
}

func (b *Buffer) AppendBuffer(buffer *Buffer) *Buffer {
	return b.AppendBytes(buffer.Data)
}

func (b *Buffer) AppendLength(length uint32) *Buffer {
	if length <= 127 {
		return b.AppendUint8(uint8(length))
	}

	if length <= 16383 {
		return b.AppendUint8(uint8(length) | 0x80).
			AppendUint8(uint8(length >> 7))
	}

	if length <= 2097151 {
		return b.AppendUint8(uint8(length) | 0x80).
			AppendUint8(uint8(length>>7) | 0x80).
			AppendUint8(uint8(length >> 14))
	}

	return b.AppendUint8(uint8(length) | 0x80).
		AppendUint8(uint8(length>>7) | 0x80).
		AppendUint8(uint8(length>>14) | 0x80).
		AppendUint8(uint8(length >> 21))
}

func SplitString(src []byte) ([]byte, []byte) {
	var l uint32
	l = (uint32(src[0]) << 8) | uint32(src[1])
	if l+2 > uint32(len(src)) {
		return nil, nil
	}
	return src[2 : 2+l], src[2+l:]
}

type MqttControlPacketType byte

const (
	CONNECT     MqttControlPacketType = 0x10
	CONNACK                           = 0x20
	PUBLISH                           = 0x30
	PUBACK                            = 0x40
	PUBREC                            = 0x50
	PUBREL                            = 0x60
	PUBCOMP                           = 0x70
	SUBSCRIBE                         = 0x80
	SUBACK                            = 0x90
	UNSUBSCRIBE                       = 0xA0
	UNSUBACK                          = 0xB0
	PINGREQ                           = 0xC0
	PINGRESP                          = 0xD0
	DISCONNECT                        = 0xE0
)

var ControlPacketTypeStrings = []string{
	"RESERVED(0)",
	"CONNECT",
	"CONNACK",
	"PUBLISH",
	"PUBACK",
	"PUBREC",
	"PUBREL",
	"PUBCOMP",
	"SUBSCRIBE",
	"SUBACK",
	"UNSUBSCRIBE",
	"UNSUBACK",
	"PINGREQ",
	"PINGRESP",
	"DISCONNECT",
	"RESERVED(15)",
}

func (c MqttControlPacketType) String() string {
	return ControlPacketTypeStrings[c>>4]
}

type MqttMessage struct {
	ControlPacketType MqttControlPacketType
	Retain            bool
	Qos               byte
	Dup               bool
	VarHeader         *Buffer
	Payload           *Buffer
}

var (
	IncompleteMessageError = errors.New("Incomplete message")
)

func NewMqttMessage(control_packet_type MqttControlPacketType) *MqttMessage {
	m := &MqttMessage{}
	m.Clear()
	m.ControlPacketType = control_packet_type
	return m
}

func (m *MqttMessage) Clear() {
	m.ControlPacketType = 0
	m.Retain = false
	m.Dup = false
	m.Qos = 0
	m.VarHeader = NewBuffer()
	m.Payload = NewBuffer()
}

func (m *MqttMessage) GetPacketIdentifier() uint16 {
	l := m.VarHeader.Length()
	if l < 2 {
		return 0
	}
	return (uint16(m.VarHeader.Data[l-2]) << 8) | uint16(m.VarHeader.Data[l-1])
}

func MqttMessageWrite(dest io.Writer, m *MqttMessage) error {

	buf := NewBuffer()
	control := m.ControlPacketType
	control |= MqttControlPacketType((m.Qos & 3) << 1)
	if m.Dup {
		control |= 8
	}
	if m.Retain {
		control |= 1
	}
	buf.AppendUint8(uint8(control))
	buf.AppendLength(uint32(m.VarHeader.Length() + m.Payload.Length()))
	buf.AppendBuffer(m.VarHeader)
	buf.AppendBuffer(m.Payload)

	Logger.DebugX("Sending %s: %q", m.ControlPacketType, buf.Data)
	n, err := dest.Write(buf.Data)
	if err != nil {
		return err
	}
	if n != len(buf.Data) {
		return fmt.Errorf("Write of %s message failed, %d bytes of %d transmitted", m.ControlPacketType, n, len(buf.Data))
	}
	return nil
}

func MqttReadLength(source io.Reader) (uint32, error) {
	var b [1]byte
	var length uint32

	length = 0
	for i := uint32(0); i < 4; i++ {
		if _, err := source.Read(b[:]); err != nil {
			return 0, err
		}
		length |= uint32(b[0]&0x7F) << (i * 7)

		if b[0]&0x80 == 0 {
			return length, nil
		}
	}
	return 0, errors.New("Variable length is too long")
}

func MqttMessageRead(source io.Reader) (*MqttMessage, error) {
	var head [1]byte
	var length uint32

	if _, err := source.Read(head[:]); err != nil {
		return nil, err
	}

	m := NewMqttMessage(MqttControlPacketType(head[0] & 0xF0))
	if (head[0] & 1) != 0 {
		m.Retain = true
	}
	if (head[0] & 8) != 0 {
		m.Dup = true
	}
	m.Qos = (head[0] >> 1) & 3

	length, err := MqttReadLength(source)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	n, err := source.Read(buf)
	if n != len(buf) {
		return nil, IncompleteMessageError
	}
	if err != nil {
		return nil, err
	}

	Logger.DebugX("Got %s(0x%2x), length %d, body %q", m.ControlPacketType, head[0], length, buf)

	switch m.ControlPacketType {
	case CONNECT:
		return nil, errors.New("Unexpected CONNECT message")

	case CONNACK:
		if length != 2 {
			return nil, errors.New("CONNACK length is not 2")
		}
		m.VarHeader = NewBufferBytes(buf)

	case PUBLISH:
		var skip uint32
		if length < 2 {
			return nil, errors.New("PUBLISH var header is incomplete")
		}
		skip = 2 + (uint32(buf[0]) << 8) | uint32(buf[1])
		if m.Qos == 1 {
			skip += 2
		}
		if length < skip {
			return nil, errors.New("PUBLISH var header is incomplete")
		}
		m.VarHeader = NewBufferBytes(buf[:skip])
		m.Payload = NewBufferBytes(buf[skip:])

	case PUBACK:
		if length != 2 {
			return nil, errors.New("PUBACK length is not 2")
		}
		m.VarHeader = NewBufferBytes(buf)

	case PUBREC:
		return nil, errors.New("Unexpected PUBREC message")

	case PUBREL:
		return nil, errors.New("Unexpected PUBREL message")

	case PUBCOMP:
		return nil, errors.New("Unexpected PUBCOMP message")

	case SUBSCRIBE:
		return nil, errors.New("Unexpected SUBSCRIBE message")

	case SUBACK:
		if length < 3 {
			return nil, errors.New("SUBACK length is less than 3")
		}
		m.VarHeader = NewBufferBytes(buf[:2])
		m.Payload = NewBufferBytes(buf[2:])

	case UNSUBSCRIBE:
		if length < 2 {
			return nil, errors.New("UNSUBSCRIBE length is less than 2")
		}
		m.VarHeader = NewBufferBytes(buf[:2])
		m.Payload = NewBufferBytes(buf[2:])

	case UNSUBACK:
		if length != 2 {
			return nil, errors.New("UNSUBACK length is not 2")
		}
		m.VarHeader = NewBufferBytes(buf)

	case PINGREQ:
		return nil, errors.New("Unexpected PINGREQ message")

	case PINGRESP:
		if length != 0 {
			return nil, errors.New("PINGRESP length is not 0")
		}

	case DISCONNECT:
		if length == 0 {
			return nil, errors.New("DISCONNECT length is not 0")
		}
	default:
		return nil, fmt.Errorf("Unexpected packet type: %d", m.ControlPacketType)
	}
	return m, nil
}
