package communication

import (
	"fmt"
	"strconv"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/services"
)

type Protocol struct {
	conn        SafeSocket
	clientId    string
	lengthBytes int
}

func NewProtocol(conn SafeSocket, clientId string, lengthBytes int) *Protocol {
	return &Protocol{
		conn:        conn,
		clientId:    clientId,
		lengthBytes: lengthBytes,
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
