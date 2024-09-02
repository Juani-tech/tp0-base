package common

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
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
	c.conn = conn
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
			err := c.SendAll(message)

			if err != nil {
				log.Debugf("action: send_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			msg, err := bufio.NewReader(c.conn).ReadString('\n')
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

			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

// Tries to send all the bytes in string, returns the error raised if there is one
func (c *Client) SendAll(message string) error {
	for bytesSent := 0; bytesSent < len(message); {
		select {
		case <-c.stop:
			log.Debugf("action: send_all | result: interrupted | client_id: %v", c.config.ID)
			return errors.New("sigterm received")
		default:
			bytes, err := fmt.Fprint(
				c.conn,
				message[bytesSent:],
			)

			if err != nil {
				log.Debugf("action: send_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return err
			}

			bytesSent += bytes
		}
	}
	return nil
}

func (c *Client) SendBet(g *Bet) {
	// Protocol:
	// 	- csv information with key=value format, and \n to delimit the message
	// 	- Example:
	// NOMBRE=Juan,APELLIDO=Perez,DOCUMENTO=11111111,NACIMIENTO=2020-03-03,NUMERO=1234\n
	err := c.createClientSocket()
	if err != nil {
		log.Debugf("action: send_bet | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}
	message :=
		fmt.Sprintf("AGENCIA=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s\n", c.config.ID, g.name, g.surname, g.document, g.birthDate, g.gambledNumber)

	err = c.SendAll(message)

	if err != nil {
		log.Debugf("action: send_bet | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}

	log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", g.document, g.gambledNumber)
}

// maxBatchSize represents the maximum amount of bytes sent per message
func (c *Client) SendBatchesOfBets(batchesOfBets []Batch, maxMessageSize int) error {
	var message string
	for _, batch := range batchesOfBets {
		err := c.createClientSocket()
		defer c.conn.Close()

		if err != nil {
			log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		message, err = c.formatBatch(batch)

		if err != nil {
			log.Debugf("action: format_batch | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		err = c.sendMessageWithMaxSize(message, maxMessageSize)

		if err != nil {
			log.Debugf("action: send_batches_of_bets | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		msg, err := bufio.NewReader(c.conn).ReadString('\n')
		c.conn.Close()

		if err != nil {
			log.Debugf("action: close_socket | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		log.Debugf("action: server_response | result: success | client_id: %v | response: %v",
			c.config.ID,
			msg,
		)

	}
	return nil
}

func betHasDelimiters(record []string) bool {
	for _, value := range record {
		if strings.Contains(value, "\n") || strings.Contains(value, ":") || strings.Contains(value, ",") {
			log.Debugf("The value: %s contains an invalid character (\\n or :)", value)
			return true
		}
	}
	return false
}

/*
Formats a batch to a protocol message style (csv with key=value)
Observation:
  - Added a ":" at the end of each bet (except for the last one) in order to separate them
  - Added the size of the batch
*/
func (c *Client) formatBatch(b Batch) (string, error) {
	formattedMessage := fmt.Sprintf("%d,", len(b))

	for i, record := range b {
		select {
		case <-c.stop:
			log.Debugf("action: send_all | result: interrupted | client_id: %v", c.config.ID)
			return "", errors.New("sigterm received")

		default:
			if betHasDelimiters(record) {
				log.Debugf("Invalid record: %s", record)
				continue
			}
			if i == len(b)-1 {
				// Do not add ':' to our last record
				formattedMessage +=
					fmt.Sprintf("AGENCIA=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s", c.config.ID, record[0], record[1], record[2], record[3], record[4])
			} else {
				formattedMessage +=
					fmt.Sprintf("AGENCIA=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s:", c.config.ID, record[0], record[1], record[2], record[3], record[4])
			}
		}

	}
	formattedMessage += "\n"
	return formattedMessage, nil
}

// There's no min func until go 1.21!! (using version 1.17) D:
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Sends a given message in chunks of maximum bytes: maxMessageSize
func (c *Client) sendMessageWithMaxSize(message string, maxMessageSize int) error {
	index := 0
	var nextIndex int
	for {
		nextIndex = min(index+maxMessageSize, len(message))
		err := c.SendAll(message[index:nextIndex])

		if err != nil {
			log.Debugf("action: send_message_with_max_size | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		if nextIndex == len(message) {
			break
		}
		index = nextIndex
	}

	return nil
}
