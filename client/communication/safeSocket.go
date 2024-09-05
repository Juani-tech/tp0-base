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

// RecvAll reads a message from the connection. It first reads a fixed-length header that indicates the
// length of the message, then reads the actual message.
//
// Returns the message as a string, or an error if something goes wrong during reading.
func (s *SafeSocket) RecvAll() (string, error) {
	totalMessage := ""

	for {
		select {
		case <-s.stop:
			return "", errors.New("sigterm received")
		default:
			s.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

			reader := bufio.NewReader(s.conn)
			buffer := make([]byte, s.lengthBytes)
			bytesRead, err := reader.Read(buffer)

			if err != nil || bytesRead != s.lengthBytes {
				if errors.Is(err, os.ErrDeadlineExceeded) {
					continue
				}
				return "", err
			}

			s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			str := string(buffer)

			length, err := strconv.Atoi(str)
			if err != nil {
				return "", err
			}

			msgBuffer := make([]byte, length)
			bytesRead, err = reader.Read(msgBuffer)

			if err != nil || bytesRead < length {
				return "", err
			}

			// Accumulate the message buffer into totalMessage
			totalMessage += string(msgBuffer[:bytesRead])

			// Check if the accumulated message contains a newline
			if strings.Contains(totalMessage, "\n") {
				// Trim the \n and return the full message
				return strings.TrimSuffix(totalMessage, "\n"), nil
			}
		}
	}
}

// Close closes the underlying network connection.
func (s *SafeSocket) Close() {
	s.conn.Close()
}
