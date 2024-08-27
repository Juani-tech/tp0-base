package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	stop := make(chan bool, 1)

	// goroutine to handle the signal and trigger shutdown
	// it has to be goroutine because the channels otherwise would block the program
	// until SIGTERM is received
	go func() {
		sig := <-sigs
		log.Infof("action: signal_received | result: success | signal: %v | client_id: %v", sig, c.config.ID)
		stop <- true
	}()

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed

	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		// As tour of go says:
		// The select statement lets a goroutine wait on multiple communication operations.
		select {
		case <-stop:
			log.Infof("action: loop_terminated | result: interrupted | client_id: %v", c.config.ID)
			c.conn.Close()
			return
		default:
			c.createClientSocket()

			message := fmt.Sprintf("[CLIENT %v] Message N°%v\n", c.config.ID, msgID)
			err := c.SendAll(message)

			if err != nil {
				log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			msg, err := bufio.NewReader(c.conn).ReadString('\n')
			c.conn.Close()

			if err != nil {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
				c.config.ID,
				msg,
			)

			c.conn.Close()
			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

// Tries to send all the bytes in string, returns the error raised if there is one
func (c *Client) SendAll(message string) error {
	log.Infof("message: %s", message)
	for bytes_sent := 0; bytes_sent < len(message); {
		bytes, err := fmt.Fprint(
			c.conn,
			message,
		)

		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		bytes_sent += bytes
	}
	return nil
}
