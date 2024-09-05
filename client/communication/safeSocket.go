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

type SafeSocket struct {
	conn        net.Conn
	stop        chan bool
	lengthBytes int
}

var log = logging.MustGetLogger("log")

func NewSafeSocket(conn net.Conn, stop chan bool, lengthBytes int) *SafeSocket {
	return &SafeSocket{
		conn:        conn,
		stop:        stop,
		lengthBytes: lengthBytes,
	}
}

// Tries to send all the bytes in string, returns the error raised if there is one
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

func (s *SafeSocket) RecvAllWithLengthBytes() (string, error) {
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

func (s *SafeSocket) Close() {
	s.conn.Close()
}
