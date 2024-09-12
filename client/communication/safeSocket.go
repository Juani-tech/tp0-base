package communication

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/op/go-logging"
)

// SafeSocket wraps a net.Conn and provides safe methods for sending and receiving data,
// respecting SIGTERM signals via a stop channel.
type SafeSocket struct {
	conn        net.Conn
	stop        chan bool
	lengthBytes int
}

var log = logging.MustGetLogger("log")

// NewSafeSocket creates and returns a new SafeSocket instance.
//
// Parameters:
// - conn: the underlying network connection.
// - stop: a channel used to signal when to stop processing (e.g., on SIGTERM).
// - lengthBytes: the number of bytes that represent the message length.
//
// Returns a pointer to the SafeSocket instance.
func NewSafeSocket(conn net.Conn, stop chan bool, lengthBytes int) *SafeSocket {
	return &SafeSocket{
		conn:        conn,
		stop:        stop,
		lengthBytes: lengthBytes,
	}
}

// SendAll sends the entire message over the network connection. It handles partial writes
// by continuing until the entire message is sent.
//
// Parameters:
// - message: the string message to be sent.
//
// Returns an error if the operation fails, either due to the connection being interrupted or any other error.
func (s *SafeSocket) SendAll(message string) error {
	for bytesSent := 0; bytesSent < len(message); {
		select {
		case <-s.stop:
			log.Debugf("action: send_all | result: interrupted")
			return errors.New("sigterm received")
		default:
			bytes, err := fmt.Fprint(
				s.conn,
				message[bytesSent:],
			)

			if err != nil {
				return err
			}

			bytesSent += bytes
		}
	}
	return nil
}

func (s *SafeSocket) readLength(reader *bufio.Reader) (int, error) {
	lengthBuffer := make([]byte, 0)
	for len(lengthBuffer) < s.lengthBytes {
		select {
		case <-s.stop:
			return 0, errors.New("sigterm received")
		default:
			s.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

			buffer := make([]byte, s.lengthBytes-len(lengthBuffer))

			bytesRead, err := reader.Read(buffer)

			s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			if err != nil {
				if errors.Is(err, os.ErrDeadlineExceeded) {
					continue
				}
				return 0, err
			}

			if bytesRead > 0 {
				lengthBuffer = append(lengthBuffer, buffer[:bytesRead]...)
			}
		}
	}
	str := string(lengthBuffer)

	length, err := strconv.Atoi(str)
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (s *SafeSocket) readNBytes(n int, reader *bufio.Reader) (string, error) {
	totalMessage := ""

	for len(totalMessage) < n {
		select {
		case <-s.stop:
			return "", errors.New("sigterm received")
		default:
			s.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

			msgBuffer := make([]byte, n-len(totalMessage))
			bytesRead, err := reader.Read(msgBuffer)

			s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			if err != nil {
				if errors.Is(err, os.ErrDeadlineExceeded) {
					continue
				}
				return "", err
			}

			// Accumulate the message buffer into totalMessage
			totalMessage += string(msgBuffer[:bytesRead])
		}
	}
	// Trim the \n and return the full message
	return strings.TrimSuffix(totalMessage, "\n"), nil
}

// RecvAll reads a message from the connection. It first reads a fixed-length header that indicates the
// length of the message, then reads the actual message.
//
// Returns the message as a string, or an error if something goes wrong during reading.
func (s *SafeSocket) RecvAllWithLengthBytes() (string, error) {
	reader := bufio.NewReader(s.conn)

	length, err := s.readLength(reader)
	if err != nil {
		return "", err
	}

	message, err := s.readNBytes(length, reader)
	if err != nil {
		return "", err
	}

	return message, nil
}

// Close closes the underlying network connection.
func (s *SafeSocket) Close() {
	s.conn.Close()
}
