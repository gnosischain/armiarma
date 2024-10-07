package clickhouse

import (
	"fmt"

	"github.com/spf13/viper"
)

func LoadClickHouseConfig() (*ClickHouseConfig, error) {
	config := &ClickHouseConfig{
		Address:  viper.GetString("clickhouse.address"),
		Database: viper.GetString("clickhouse.database"),
		Username: viper.GetString("clickhouse.username"),
		Password: viper.GetString("clickhouse.password"),
	}

	if config.Address == "" || config.Database == "" || config.Username == "" || config.Password == "" {
		return nil, fmt.Errorf("missing required ClickHouse configuration")
	}

	return config, nil
}