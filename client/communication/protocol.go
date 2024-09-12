package communication

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/services"
)

type Protocol struct {
	conn           SafeSocket
	batchSize      int
	stop           chan bool
	clientId       string
	maxMessageSize int
	lengthBytes    int
}

func NewProtocol(conn SafeSocket, batchSize int, stop chan bool, clientId string, maxMessageSize int, lengthBytes int) *Protocol {
	return &Protocol{
		conn:           conn,
		batchSize:      batchSize,
		stop:           stop,
		clientId:       clientId,
		maxMessageSize: maxMessageSize,
		lengthBytes:    lengthBytes,
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
	return s
}

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
	formattedMessage := fmt.Sprintf("%d,", len(b))

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
