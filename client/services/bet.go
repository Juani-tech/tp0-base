package services

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strings"

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

func BatchOfBetsFromCsvFileManualAtPosition(filePath string, batchSize int, startPosition int64) (Batch, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		// log.Printf("action: open_csv | result: fail | filepath: %s | err: %s", filePath, err)
		return nil, 0, err
	}
	defer file.Close()

	// Seek to the specified start position
	_, err = file.Seek(startPosition, io.SeekStart)
	if err != nil {
		// log.Debugf("action: seek_csv | result: fail | filepath: %s | err: %s", filePath, err)
		return nil, 0, err
	}

	reader := bufio.NewReader(file)
	batch := make(Batch, 0)
	bytesRead := startPosition // Start the byte count from the seek position

	for {
		// Read each line (representing a CSV row)
		line, err := reader.ReadString('\n')
		bytesRead += int64(len(line))

		// Check if EOF is reached
		if err != nil {
			if err.Error() == "EOF" && len(batch) > 0 {
				// EOF reached but we have data in the batch to return
				return batch, bytesRead, io.EOF
			}
			// Return any other error encountered
			log.Debugf("action: read_csv | result: fail | filepath: %s | err: %s", filePath, err)
			return nil, bytesRead, err
		}

		// Parse the CSV line by splitting on commas (simple parsing for demo purposes)
		record := strings.Split(strings.TrimSpace(line), ",")
		batch = append(batch, record)

		// Return the batch if the size limit is reached
		if len(batch) == batchSize {
			return batch, bytesRead, nil
		}
	}

}
