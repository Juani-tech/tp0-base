package services

import (
	"encoding/csv"
	"io"
	"os"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

type Bet struct {
	Name          string
	Surname       string
	Document      string
	BirthDate     string
	GambledNumber string
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

	log.Debugf("action: config_Bet | result: success | name: %s | surname: %s | document: %v | birthDate: %v | number: %s",
		v.GetString("nombre"),
		v.GetString("apellido"),
		v.GetString("documento"),
		// v.GetString("nacimiento"),
		os.Getenv("CLI_NACIMIENTO"),
		v.GetString("numero"),
	)

	return &Bet{
		Name:          v.GetString("nombre"),
		Surname:       v.GetString("apellido"),
		Document:      v.GetString("documento"),
		BirthDate:     os.Getenv("CLI_NACIMIENTO"),
		GambledNumber: v.GetString("numero"),
	}
}

// Returns the contents of a csv file in batchs of `batchSize`
func BatchOfBetsFromCsvFile(filePath string, batchSize int) ([]Batch, error) {
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
		record, err := reader.Read()
		if err == io.EOF {
			if len(actualBatch) > 0 {
				batchesOfBets = append(batchesOfBets, actualBatch)
			}
			break
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

	return batchesOfBets, nil
}
