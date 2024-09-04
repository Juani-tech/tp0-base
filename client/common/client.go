package common

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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

			// msg, err := bufio.NewReader(c.conn).ReadString('\n')
			msg, err := c.RecvAll()
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

	batchesOfBets, err := BatchOfBetsFromCsvFile("./data.csv", c.config.BatchSize, c.stop)

	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	err = c.SendBatchesOfBets(batchesOfBets)
	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	log.Debugf("Notifying end of batches")
	err = c.NotifyEndOfBatches()
	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	log.Debugf("Asking for winners")
	err = c.AskForWinners()

	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	c.conn.Close()

	return nil
}

// Tries to send all the bytes in string, returns the error raised if there is one
func (c *Client) SendAll(message string) error {
	for bytesSent := 0; bytesSent < len(message); {
		select {
		case <-c.stop:
			log.Debugf("action: send_all | result: interrupted")
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

func (c *Client) RecvAll() (string, error) {
	for {
		select {
		case <-c.stop:
			return "", errors.New("sigterm received")
		default:
			c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

			reader := bufio.NewReader(c.conn)
			buffer := make([]byte, c.config.LengthBytes)
			bytesRead, err := reader.Read(buffer)

			if err != nil || bytesRead != c.config.LengthBytes {
				// if err, ok := err.(net.Error); ok && err.Timeout() {
				// 	// Continue to the next iteration if the read timed out
				// }
				if errors.Is(err, os.ErrDeadlineExceeded) {
					continue
				}
				return "", err
			}

			c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

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

			// Trim the \n from the end
			return string(msgBuffer[:bytesRead-1]), nil
		}
	}
}

func (c *Client) SendBet(g *Bet) {
	// Protocol:
	// 	- csv information with key=value format, and \n to delimit the message
	// 	- Example:
	// NOMBRE=Juan,APELLIDO=Perez,DOCUMENTO=11111111,NACIMIENTO=2020-03-03,NUMERO=1234\n
	err := c.createClientSocket()
	defer c.conn.Close()

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

	c.conn.Close()
	log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", g.document, g.gambledNumber)
}

// maxBatchSize represents the maximum amount of bytes sent per message
func (c *Client) SendBatchesOfBets(batchesOfBets []Batch) error {
	for _, batch := range batchesOfBets {
		// err := c.createClientSocket()
		// defer c.conn.Close()

		// if err != nil {
		// 	log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
		// 		c.config.ID,
		// 		err,
		// 	)
		// 	return err
		// }

		message, err := c.formatBatch(batch)

		if err != nil {
			log.Debugf("action: format_batch | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		err = c.sendMessageWithMaxSize(message)

		if err != nil {
			log.Debugf("action: send_batches_of_bets | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		// msg, err := bufio.NewReader(c.conn).ReadString('\n')
		msg, err := c.RecvAll()
		// c.conn.Close()

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
	formattedMessage := fmt.Sprintf("BATCH,%d,", len(b))

	for i, record := range b {
		select {
		case <-c.stop:
			log.Debugf("action: send_message | result: interrupted")
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

func (c *Client) formatLength(length int) string {
	s := strconv.Itoa(length)
	for {
		if len(s) < c.config.LengthBytes {
			s = "0" + s
		} else {
			break
		}
	}
	return s
}

// Sends a given message in chunks of maximum bytes: maxMessageSize
func (c *Client) sendMessageWithMaxSize(message string) error {
	index := 0
	var nextIndex int
	for {
		nextIndex = min(index+c.config.MaxMessageSize, len(message))
		// Send the length (max of 6 chars) and the message along with it
		protocolMessage := fmt.Sprintf("%s%s", c.formatLength(len(message[index:nextIndex])), message[index:nextIndex])
		err := c.SendAll(protocolMessage)

		if err != nil {
			log.Debugf("action: send_message_with_max_size | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return err
		}

		if nextIndex == len(message) {
			return nil
		}
		index = nextIndex
	}

}

func (c *Client) NotifyEndOfBatches() error {
	// err := c.createClientSocket()
	// defer c.conn.Close()

	// if err != nil {
	// 	log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
	// 		c.config.ID,
	// 		err,
	// 	)
	// 	return err
	// }

	message := fmt.Sprintf("FIN,AGENCIA=%s\n", c.config.ID)
	err := c.sendMessageWithMaxSize(message)

	// c.conn.Close()

	if err != nil {
		log.Debugf("action: notify_server | result: fail | error: %v", err)
		return err
	}

	return nil
}

func (c *Client) parseWinners(message string) (uint32, error) {
	values := strings.Split(message, ",")
	amountOfWinners, err := strconv.Atoi(values[0])

	if err != nil {
		log.Debugf("Error parsing amount of winners: %v", err)
		return 0, err
	}

	if amountOfWinners == 0 {
		return 0, nil
	}

	winnersDocuments := values[1:]

	if len(winnersDocuments) != amountOfWinners {
		err := fmt.Errorf("expected amount of winners: %d, got: %d", amountOfWinners, len(winnersDocuments))
		return 0, err
	}

	return uint32(amountOfWinners), nil
}

func (c *Client) AskForWinners() error {
	// for {
	// select {
	// case <-c.stop:
	// 	log.Debugf("action: ask_winners | result: interrupted")
	// 	return errors.New("sigterm received")
	// default:
	// err := c.createClientSocket()
	// defer c.conn.Close()

	// if err != nil {
	// 	log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
	// 		c.config.ID,
	// 		err,
	// 	)
	// 	return err
	// }

	message := fmt.Sprintf("GANADORES,AGENCIA=%s\n", c.config.ID)

	err := c.sendMessageWithMaxSize(message)
	if err != nil {
		log.Debugf("action: ask_winners | result: fail | error: %v", err)
		return err
	}

	// msg, err := bufio.NewReader(c.conn).ReadString('\n')
	msg, err := c.RecvAll()

	// c.conn.Close()

	// if err == io.EOF {
	// time.Sleep(1 * time.Second)
	// continue
	// } else
	if err != nil {
		log.Debugf("action: receive_winners | result: fail | error: %v", err)
		return err
	}

	amountOfWinners, err := c.parseWinners(msg)

	if err != nil {
		log.Debugf("action: parse_winners | result: fail | error: %v", err)
		return err
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", amountOfWinners)
	// }

	return nil
	// }

}
