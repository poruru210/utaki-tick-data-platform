package fake

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	protocolv1 "tick-data-platform/protocol/v1/go"
)

// Client is a small deterministic Protocol V1 producer used by the local TCP tests.
// It keeps the same session identity across reconnects so an in-flight batch can be retried byte-for-byte.
type Client struct {
	conn    net.Conn
	Address string
	Hello   protocolv1.HelloV1
	Resume  protocolv1.ResumeV1
}

func Dial(ctx context.Context, address string, hello protocolv1.HelloV1) (*Client, error) {
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: connection, Address: address, Hello: hello}
	if err := client.handshake(); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return client, nil
}

func New(conn net.Conn, hello protocolv1.HelloV1) (*Client, error) {
	client := &Client{conn: conn, Hello: hello}
	if err := client.handshake(); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) Conn() net.Conn { return c.conn }

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) Reconnect(ctx context.Context, address string) error {
	_ = c.Close()
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	c.conn = connection
	c.Address = address
	if err := c.handshake(); err != nil {
		_ = connection.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) SendBatch(batch protocolv1.BatchFrameV1) (protocolv1.AckV1, error) {
	if batch.SessionLeaseID == "" {
		batch.SessionLeaseID = c.Resume.SessionLeaseID
	}
	frame, err := protocolv1.EncodeMessage(batch)
	if err != nil {
		return protocolv1.AckV1{}, err
	}
	return c.SendRawBatch(frame)
}

func (c *Client) SendRawBatch(frame []byte) (protocolv1.AckV1, error) {
	if c.conn == nil {
		return protocolv1.AckV1{}, fmt.Errorf("fake producer is disconnected")
	}
	if err := writeAll(c.conn, frame); err != nil {
		return protocolv1.AckV1{}, err
	}
	message, err := c.readMessage()
	if err != nil {
		return protocolv1.AckV1{}, err
	}
	ack, ok := message.(protocolv1.AckV1)
	if !ok {
		if gatewayError, isError := message.(protocolv1.ErrorV1); isError {
			return protocolv1.AckV1{}, fmt.Errorf("gateway error %d: %s", gatewayError.Code, gatewayError.Message)
		}
		return protocolv1.AckV1{}, fmt.Errorf("expected AckV1, got %T", message)
	}
	return ack, nil
}

func (c *Client) handshake() error {
	frame, err := protocolv1.EncodeMessage(c.Hello)
	if err != nil {
		return err
	}
	if err := writeAll(c.conn, frame); err != nil {
		return err
	}
	message, err := c.readMessage()
	if err != nil {
		return err
	}
	resume, ok := message.(protocolv1.ResumeV1)
	if !ok {
		if gatewayError, isError := message.(protocolv1.ErrorV1); isError {
			return fmt.Errorf("gateway hello error %d: %s", gatewayError.Code, gatewayError.Message)
		}
		return fmt.Errorf("expected ResumeV1, got %T", message)
	}
	c.Resume = resume
	return nil
}

func (c *Client) readMessage() (protocolv1.Message, error) {
	var header [16]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, err
	}
	frameLength := binary.LittleEndian.Uint32(header[8:12])
	if frameLength < 20 || frameLength > protocolv1.MaxFrameBytes {
		return nil, fmt.Errorf("invalid gateway frame length %d", frameLength)
	}
	raw := make([]byte, frameLength)
	copy(raw[:16], header[:])
	if _, err := io.ReadFull(c.conn, raw[16:]); err != nil {
		return nil, err
	}
	frame, err := protocolv1.DecodeFrame(raw)
	if err != nil {
		return nil, err
	}
	return protocolv1.DecodeMessage(frame)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
