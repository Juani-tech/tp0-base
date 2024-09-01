package common

import (
	"encoding/csv"
	"errors"
	"io"
	"os"

	"github.com/spf13/viper"
)

type Bet struct {
	name          string
	surname       string
	document      string
	birthDate     string
	gambledNumber string
}

type Batch [][]string

func NewBetFromENV() *Bet {
	v := viper.New()
	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.BindEnv("nombre")
	v.BindEnv("apellido")
	v.BindEnv("documento")
	v.BindEnv("nacimiento")
	v.BindEnv("numero")

	log.Debugf("action: config_bet | result: success | name: %s | surname: %s | document: %v | birthDate: %v | number: %s",
		v.GetString("nombre"),
		v.GetString("apellido"),
		v.GetString("documento"),
		v.GetString("nacimiento"),
		v.GetString("numero"),
	)

	return &Bet{
		name:          v.GetString("nombre"),
		surname:       v.GetString("apellido"),
		document:      v.GetString("documento"),
		birthDate:     v.GetString("nacimiento"),
		gambledNumber: v.GetString("numero"),
	}
}

// Returns the contents of a csv file in batchs of `batchSize`
func BatchOfBetsFromCsvFile(filePath string, batchSize int, c chan bool) ([]Batch, error) {
	file, err := os.Open(filePath)

	if err != nil {
		log.Debugf("action: open_csv | result: fail | filepath: %s | err: %s", filePath, err)
		return nil, err
	}

	// defer: execute at the end of the function
	defer file.Close()

	reader := csv.NewReader(file)

	batchesOfBets := make([]Batch, 0)
	actualBatch := make(Batch, 0)
	for {
		select {
		case <-c:
			log.Debugf("action: read_csv | result: interrupted")
			file.Close()
			return nil, errors.New("sigterm received")
		default:
			record, err := reader.Read()
			if err == io.EOF {
				if len(actualBatch) > 0 {
					batchesOfBets = append(batchesOfBets, actualBatch)
				}
				return batchesOfBets, nil
			}

			if err != nil {
				log.Debugf("action: read_csv | result: fail | filepath: %s | err: %s", filePath, err)
				return nil, err
			}

			actualBatch = append(actualBatch, record)

			if len(actualBatch) == batchSize {
				batchesOfBets = append(batchesOfBets, actualBatch)
				actualBatch = make(Batch, 0)
			}
		}
	}

}
