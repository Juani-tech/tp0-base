package services

import (
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
