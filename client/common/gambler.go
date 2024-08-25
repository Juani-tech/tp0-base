package common

import (
	"github.com/spf13/viper"
)

type Gambler struct {
	name          string
	surname       string
	document      string
	birthDate     string
	gambledNumber string
}

func NewGamblerFromENV() *Gambler {
	v := viper.New()
	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")

	v.BindEnv("nombre")
	v.BindEnv("apellido")
	v.BindEnv("documento")
	v.BindEnv("nacimiento")
	v.BindEnv("numero")

	log.Infof("action: config_gambler | result: success | name: %s | surname: %s | document: %v | birthDate: %v | number: %s",
		v.GetString("nombre"),
		v.GetString("apellido"),
		v.GetString("documento"),
		v.GetString("nacimiento"),
		v.GetString("numero"),
	)

	return &Gambler{
		name:          v.GetString("nombre"),
		surname:       v.GetString("apellido"),
		document:      v.GetString("documento"),
		birthDate:     v.GetString("nacimiento"),
		gambledNumber: v.GetString("numero"),
	}
}
