package communication

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/services"
)

type Protocol struct {
	conn           SafeSocket
	batchSize      int
	stop           chan bool
	clientId       string
	maxMessageSize int
	lengthBytes    int
	serverAddress  string
}

func NewProtocol(conn SafeSocket, batchSize int, stop chan bool, clientId string, maxMessageSize int, lengthBytes int, serverAddress string) *Protocol {
	return &Protocol{
		conn:           conn,
		batchSize:      batchSize,
		stop:           stop,
		clientId:       clientId,
		maxMessageSize: maxMessageSize,
		lengthBytes:    lengthBytes,
		serverAddress:  serverAddress,
	}
}

func (p *Protocol) SendBet(b *services.Bet) error {
	// Protocol:
	// 	- csv information with key=value format, and \n to delimit the message
	// 	- Example:
	// NOMBRE=Juan,APELLIDO=Perez,DOCUMENTO=11111111,NACIMIENTO=2020-03-03,NUMERO=1234\n

	message :=
		fmt.Sprintf("AGENCIA=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s\n", p.clientId, b.Name, b.Surname, b.Document, b.BirthDate, b.GambledNumber)

	err := p.conn.SendAll(p.formatLength(len(message)) + message)

	if err != nil {
		return err
	}
	return nil
}

func (p *Protocol) formatLength(length int) string {
	s := strconv.Itoa(length)
	for {
		if len(s) < p.lengthBytes {
			s = "0" + s
		} else {
			break
		}
	}
	log.Debugf("Length: %d | formattedLenght: %s", length, s)
	return s
}

func (p *Protocol) RunProtocol() error {
	// batchesOfBets, err := services.BatchOfBetsFromCsvFile("./data.csv", p.batchSize, p.stop)

	// if err != nil {
	// 	log.Debugf("%s", err)
	// 	return err
	// }

	err := p.SendBatchesOfBets("./data.csv")
	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	err = p.NotifyEndOfBatches()
	if err != nil {
		log.Debugf("%s", err)
		return err
	}

	p.conn.Close()

	err = p.AskForWinners()

	if err != nil {
		log.Debugf("%s", err)
		return err
	}
	return nil
}

// // maxBatchSize represents the maximum amount of bytes sent per message
// func (p *Protocol) SendBatchesOfBets(batchesOfBets []services.Batch) error {
// 	for _, batch := range batchesOfBets {
// 		message, err := p.formatBatch(batch)

// 		if err != nil {
// 			log.Debugf("action: format_batch | result: fail | client_id: %v | error: %v",
// 				p.clientId,
// 				err,
// 			)
// 			return err
// 		}

// 		err = p.sendMessageWithMaxSize(message)

// 		if err != nil {
// 			log.Debugf("action: send_batches_of_bets | result: fail | client_id: %v | error: %v",
// 				p.clientId,
// 				err,
// 			)
// 			return err
// 		}

// 		msg, err := p.conn.RecvAllWithLengthBytes()

// 		if err != nil {
// 			log.Debugf("action: server_response | result: fail | client_id: %v | error: %v",
// 				p.clientId,
// 				err,
// 			)
// 			return err
// 		}

// 		log.Debugf("action: server_response | result: success | client_id: %v | response: %v",
// 			p.clientId,
// 			msg,
// 		)

// 	}
// 	return nil
// }

func (p *Protocol) SendBatchesOfBets(filePath string) error {
	var currentPosition int64 = 0
	for {
		batch, nextPosition, errStop := services.BatchOfBetsFromCsvFileManualAtPosition(filePath, p.batchSize, currentPosition)

		if errStop == io.EOF && len(batch) == 0 {
			return nil
		} else if errStop != nil && errStop != io.EOF {
			return errStop
		}

		currentPosition = nextPosition

		message, err := p.formatBatch(batch)

		if err != nil {
			log.Debugf("action: format_batch | result: fail | client_id: %v | error: %v",
				p.clientId,
				err,
			)
			return err
		}

		err = p.sendMessageWithMaxSize(message)

		if err != nil {
			log.Debugf("action: send_batches_of_bets | result: fail | client_id: %v | error: %v",
				p.clientId,
				err,
			)
			return err
		}

		msg, err := p.conn.RecvAllWithLengthBytes()

		if err != nil {
			log.Debugf("action: server_response | result: fail | client_id: %v | error: %v",
				p.clientId,
				err,
			)
			return err
		}

		log.Debugf("action: server_response | result: success | client_id: %v | response: %v",
			p.clientId,
			msg,
		)
		if errStop != nil {
			break
		}
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
func (p *Protocol) formatBatch(b services.Batch) (string, error) {
	formattedMessage := fmt.Sprintf("BATCH,%d,", len(b))

	for i, record := range b {
		select {
		case <-p.stop:
			log.Debugf("action: send_all | result: interrupted | client_id: %v", p.clientId)
			return "", errors.New("sigterm received")

		default:
			if betHasDelimiters(record) {
				log.Debugf("Invalid record: %s", record)
				continue
			}
			if i == len(b)-1 {
				// Do not add ':' to our last record
				formattedMessage +=
					fmt.Sprintf("AGENCIA=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s", p.clientId, record[0], record[1], record[2], record[3], record[4])
			} else {
				formattedMessage +=
					fmt.Sprintf("AGENCIA=%s,NOMBRE=%s,APELLIDO=%s,DOCUMENTO=%s,NACIMIENTO=%s,NUMERO=%s:", p.clientId, record[0], record[1], record[2], record[3], record[4])
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
func (p *Protocol) sendMessageWithMaxSize(message string) error {
	index := 0
	var nextIndex int
	for {
		nextIndex = min(index+p.maxMessageSize, len(message))
		// Send the length (max of 6 chars) and the message along with it
		protocolMessage := fmt.Sprintf("%s%s", p.formatLength(len(message[index:nextIndex])), message[index:nextIndex])

		err := p.conn.SendAll(protocolMessage)

		if err != nil {
			log.Debugf("action: send_message_with_max_size | result: fail | client_id: %v | error: %v",
				p.clientId,
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
func (p *Protocol) NotifyEndOfBatches() error {
	message := fmt.Sprintf("FIN,AGENCIA=%s\n", p.clientId)
	err := p.sendMessageWithMaxSize(message)

	if err != nil {
		log.Debugf("action: notify_server | result: fail | error: %v", err)
		return err
	}

	return nil
}

func (c *Protocol) parseWinners(message string) (uint32, error) {
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

func (p *Protocol) CreateProtocolSocket() error {
	conn, err := net.Dial("tcp", p.serverAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			p.clientId,
			err,
		)
		// Added this return (needed)
		return err
	}
	p.conn = *NewSafeSocket(conn, p.stop, p.lengthBytes)
	return nil
}

func (p *Protocol) AskForWinners() error {
	for {
		select {
		case <-p.stop:
			log.Debugf("action: ask_winners | result: interrupted")
			return errors.New("sigterm received")
		default:
			err := p.CreateProtocolSocket()
			defer p.conn.Close()

			if err != nil {
				log.Debugf("action: create_client_socket | result: fail | client_id: %v | error: %v",
					p.clientId,
					err,
				)
				return err
			}

			message := fmt.Sprintf("GANADORES,AGENCIA=%s\n", p.clientId)

			err = p.sendMessageWithMaxSize(message)
			if err != nil {
				log.Debugf("action: ask_winners | result: fail | error: %v", err)
				return err
			}

			// msg, err := bufio.NewReader(c.conn).ReadString('\n')
			msg, err := p.conn.RecvAllWithLengthBytes()

			p.conn.Close()

			if err == io.EOF {
				time.Sleep(1 * time.Second)
				continue
			} else if err != nil {
				log.Debugf("action: receive_winners | result: fail | error: %v", err)
				return err
			}

			amountOfWinners, err := p.parseWinners(msg)

			if err != nil {
				log.Debugf("action: parse_winners | result: fail | error: %v", err)
				return err
			}

			log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", amountOfWinners)
		}

		return nil
	}

}
