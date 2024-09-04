package common

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/communication"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	BatchSize      int
	MaxMessageSize int
	LengthBytes    int
}

// Client Entity that encapsulates how
type Client struct {
	config   ClientConfig
	conn     communication.SafeSocket
	stop     chan bool
	protocol communication.Protocol
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	stop := make(chan bool, 1)

	client := &Client{
		config: config,
		stop:   stop,
	}
	// goroutine to handle the signal and trigger shutdown
	// it has to be goroutine because the channels otherwise would block the program
	// until SIGTERM is received
	go func() {
		sig := <-sigs
		log.Debugf("action: signal_received | result: success | signal: %v | client_id: %v", sig, config.ID)
		stop <- true
	}()

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
		// Added this return (needed)
		return err
	}
	c.conn = *communication.NewSafeSocket(conn, c.stop, c.config.LengthBytes)
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed

	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		// As tour of go says:
		// The select statement lets a goroutine wait on multiple communication operations.
		select {
		case <-c.stop:
			log.Debugf("action: loop_terminated | result: interrupted | client_id: %v", c.config.ID)
			c.conn.Close()
			return
		default:
			c.createClientSocket()
			message := fmt.Sprintf("[CLIENT %v] Message N°%v\n", c.config.ID, msgID)
			err := c.conn.SendAll(message)

			if err != nil {
				log.Debugf("action: send_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			// msg, err := bufio.NewReader(c.conn).ReadString('\n')
			msg, err := c.conn.RecvAll()
			c.conn.Close()
			// This checks the short-read, so no extra validation is needed
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

func (c *Client) RunProtocol() error {
	err := c.createClientSocket()
	defer c.conn.Close()

	if err != nil {
		log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}

	c.protocol = *communication.NewProtocol(c.conn, c.config.BatchSize, c.stop, c.config.ID, c.config.MaxMessageSize, c.config.LengthBytes)

	err = c.protocol.RunProtocol()

	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	c.conn.Close()

	return nil
}
