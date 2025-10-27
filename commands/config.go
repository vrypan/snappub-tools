package commands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage snappub configuration",
	Long:  `List, get, and set configuration parameters in ~/.config/snappub/config.yaml`,
}

var configLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List all configuration parameters",
	RunE: func(cmd *cobra.Command, args []string) error {
		settings := viper.AllSettings()
		if len(settings) == 0 {
			fmt.Println("No configuration found")
			return nil
		}

		data, err := yaml.Marshal(settings)
		if err != nil {
			return fmt.Errorf("error formatting config: %w", err)
		}
		fmt.Print(string(data))
		return nil
	},
}

var configGetCmd = &cobra.Command{
	Use:   "get <param>",
	Short: "Get a configuration parameter",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		param := args[0]
		value := viper.Get(param)
		if value == nil {
			return fmt.Errorf("parameter '%s' not found", param)
		}

		// Handle map types (like appKeys)
		if m, ok := value.(map[string]interface{}); ok {
			data, err := yaml.Marshal(m)
			if err != nil {
				return fmt.Errorf("error formatting value: %w", err)
			}
			fmt.Print(string(data))
		} else {
			fmt.Println(value)
		}
		return nil
	},
}

var configSetCmd = &cobra.Command{
	Use:   "set <param> <value>",
	Short: "Set a configuration parameter",
	Long: `Set a configuration parameter.
For nested values like appKeys, use dot notation: appKeys.alice 0x123...`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		param := args[0]
		value := args[1]

		// Ensure config directory exists
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("error getting home directory: %w", err)
		}

		configDir := filepath.Join(home, ".config", "snappub")
		if err := os.MkdirAll(configDir, 0755); err != nil {
			return fmt.Errorf("error creating config directory: %w", err)
		}

		configFile := filepath.Join(configDir, "config.yaml")

		// Load existing config or create new one
		viper.SetConfigFile(configFile)
		_ = viper.ReadInConfig() // Ignore error if file doesn't exist

		// Set the value
		viper.Set(param, value)

		// Write config file
		if err := viper.WriteConfig(); err != nil {
			// If config file doesn't exist, create it
			if err := viper.SafeWriteConfig(); err != nil {
				return fmt.Errorf("error writing config: %w", err)
			}
		}

		fmt.Printf("Set %s = %s\n", param, value)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configLsCmd)
	configCmd.AddCommand(configGetCmd)
	configCmd.AddCommand(configSetCmd)
}
