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
	// batchesOfBets, err := services.BatchOfBetsFromCsvFile("./data.csv", c.config.BatchSize, c.stop)
	// if err != nil {
	// 	log.Debugf("%s", err)
	// 	return err
	// }

	// err = c.SendBatchesOfBets(batchesOfBets)
	// if err != nil {
	// 	log.Debugf("%s", err)
	// 	return err
	// }

	// err = c.NotifyEndOfBatches()
	// if err != nil {
	// 	log.Debugf("%s", err)
	// 	return err
	// }

	// err = c.AskForWinners()
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

// maxBatchSize represents the maximum amount of bytes sent per message
// func (c *Client) SendBatchesOfBets() error {
// 	// var message string
// 	// for _, batch := range batchesOfBets {
// 	err := c.createClientSocket()
// 	defer c.conn.Close()

// 	if err != nil {
// 		log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
// 			c.config.ID,
// 			err,
// 		)
// 		return err
// 	}

// 	batchesOfBets, err := services.BatchOfBetsFromCsvFile("./data.csv", c.config.BatchSize)

// 	if err != nil {
// 		return err
// 	}

// 	protocol := communication.NewProtocol(c.conn, c.config.BatchSize, c.stop, c.config.ID, c.config.MaxMessageSize, c.config.LengthBytes)

// 	err = protocol.SendBatchesOfBets(batchesOfBets)

// 	if err != nil {
// 		log.Debugf("action: send_batches_of_bets | result: fail | client_id: %v | error: %v",
// 			c.config.ID,
// 			err,
// 		)
// 		return err
// 	}

// 	c.conn.Close()

// }

// func (c *Client) NotifyEndOfBatches() error {
// 	err := c.createClientSocket()
// 	defer c.conn.Close()

// 	if err != nil {
// 		log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
// 			c.config.ID,
// 			err,
// 		)
// 		return err
// 	}

// 	message := fmt.Sprintf("FIN,AGENCIA=%s\n", c.config.ID)
// 	err = c.sendMessageWithMaxSize(message)

// 	c.conn.Close()

// 	if err != nil {
// 		log.Debugf("action: notify_server | result: fail | error: %v", err)
// 		return err
// 	}

// 	return nil
// }

// func (c *Client) parseWinners(message string) (uint32, error) {
// 	values := strings.Split(message, ",")
// 	amountOfWinners, err := strconv.Atoi(values[0])

// 	if err != nil {
// 		log.Debugf("Error parsing amount of winners: %v", err)
// 		return 0, err
// 	}

// 	if amountOfWinners == 0 {
// 		return 0, nil
// 	}

// 	winnersDocuments := values[1:]

// 	if len(winnersDocuments) != amountOfWinners {
// 		err := fmt.Errorf("expected amount of winners: %d, got: %d", amountOfWinners, len(winnersDocuments))
// 		return 0, err
// 	}

// 	return uint32(amountOfWinners), nil
// }

// func (c *Client) AskForWinners() error {
// 	for {
// 		select {
// 		case <-c.stop:
// 			log.Debugf("action: ask_winners | result: interrupted")
// 			return errors.New("sigterm received")
// 		default:
// 			err := c.createClientSocket()
// 			defer c.conn.Close()

// 			if err != nil {
// 				log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
// 					c.config.ID,
// 					err,
// 				)
// 				return err
// 			}

// 			message := fmt.Sprintf("GANADORES,AGENCIA=%s\n", c.config.ID)

// 			err = c.sendMessageWithMaxSize(message)
// 			if err != nil {
// 				log.Debugf("action: ask_winners | result: fail | error: %v", err)
// 				return err
// 			}

// 			msg, err := bufio.NewReader(c.conn).ReadString('\n')

// 			c.conn.Close()

// 			if err == io.EOF {
// 				time.Sleep(1 * time.Second)
// 				continue
// 			} else if err != nil {
// 				log.Debugf("action: receive_winners | result: fail | error: %v", err)
// 				return err
// 			}

// 			amountOfWinners, err := c.parseWinners(msg)

// 			if err != nil {
// 				log.Debugf("action: parse_winners | result: fail | error: %v", err)
// 				return err
// 			}

// 			log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", amountOfWinners)
// 		}

// 		return nil
// 	}

// }
