package common

import (
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
	config ClientConfig
	conn   communication.SafeSocket
	stop   chan bool
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
func (c *Client) CreateClientSocket() error {
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

func (c *Client) RunProtocol() error {
	c.CreateClientSocket()
	defer c.conn.Close()

	protocol := communication.NewProtocol(c.conn, c.config.BatchSize, c.stop, c.config.ID, c.config.MaxMessageSize, c.config.LengthBytes, c.config.ServerAddress)
	err := protocol.RunProtocol()

	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	return nil
}
