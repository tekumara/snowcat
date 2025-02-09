package main

// adapted from https://github.com/DavidBrown-niche/gosnowflake-example/tree/0bcc7a5

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/peterbourgon/ff/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/snowflakedb/gosnowflake"
	"go.step.sm/crypto/pemutil"
)

func main() {
	var (
		snowflakeAccount            = flag.String("snowflake.account", "", "Account name for snowflake. Account name is not the username, see https://docs.snowflake.com/en/user-guide/admin-account-identifier for more details")
		snowflakeHost               = flag.String("snowflake.host", "", "Host name for snowflake (default: {account}.snowflakecomputing.com)")
		snowflakePort               = flag.Int("snowflake.port", 443, "Port for snowflake connection")
		snowflakeProtocol           = flag.String("snowflake.protocol", "https", "Protocol for snowflake connection (http or https)")
		snowflakeDatabase           = flag.String("snowflake.database", "", "Database name for snowflake")
		snowflakeSchema             = flag.String("snowflake.schema", "", "Schema name for snowflake")
		snowflakeUser               = flag.String("snowflake.user", "", "Username for snowflake")
		snowflakePassword           = flag.String("snowflake.password", "", "Password for snowflake. Cannot be used in conjunction with snowflake.private.key.file")
		snowflakeRole               = flag.String("snowflake.role", "", "Role for snowflake")
		snowflakePrivateKeyFile     = flag.String("snowflake.private.key.file", "", "Location of private key file used to authenticate with snowflake, pkcs8 in PEM format. Cannot be used in conjunction with snowflake.password")
		snowflakePrivateKeyPasscode = flag.String("snowflake.private.key.passcode", "", "Passcode for encrypted private key (not necessary if key is not encrypted)")
		snowflakeAuthenticator      = flag.String("snowflake.authenticator", "", "Authenticator type for snowflake (one of: externalbrowser)")
		snowflakeMaxRetryCount      = flag.Int("snowflake.max.retry.count", 7, "Specifies maximum number of subsequent retries with backoff. Use -1 for no retries, as 0 means use the default.")
	)

	// Setup zerolog
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if err := ff.Parse(flag.CommandLine, os.Args[1:], ff.WithEnvVars()); err != nil {
		log.Fatal().Err(err).Msg("Error parsing flags")
	}

	// Check flags
	var missingFlags []string
	if *snowflakeAccount == "" {
		missingFlags = append(missingFlags, "snowflake.account")
	}
	if *snowflakeUser == "" {
		missingFlags = append(missingFlags, "snowflake.user")
	}

	// Check authentication method
	authMethodCount := 0
	if *snowflakePassword != "" {
		authMethodCount++
	}
	if *snowflakePrivateKeyFile != "" {
		authMethodCount++
	}
	if *snowflakeAuthenticator != "" {
		authMethodCount++
	}
	if authMethodCount == 0 {
		missingFlags = append(missingFlags, "authentication method (one of: snowflake.password, snowflake.private.key.file, or snowflake.authenticator)")
	}
	if authMethodCount > 1 {
		log.Fatal().
			Bool("password_provided", *snowflakePassword != "").
			Bool("private_key_provided", *snowflakePrivateKeyFile != "").
			Bool("authenticator_provided", *snowflakeAuthenticator != "").
			Msg("Must provide exactly one authentication method")
	}

	if len(missingFlags) > 0 {
		log.Fatal().Msg("Missing required flags: " + strings.Join(missingFlags, ", "))
	}

	// Check protocol is valid
	if *snowflakeProtocol != "https" && *snowflakeProtocol != "http" {
		log.Fatal().Str("protocol", *snowflakeProtocol).Msg("Protocol must be either 'http' or 'https'")
	}

	// Create context that's cancelled when the program receives a SIGINT
	ctx := context.Background()
	ctx, cancel := signalHandlerContext(ctx)
	defer cancel()

	// need to convert private key to correct format if provided
	// Unfortunately need to use a third party package for this
	// because the std crypto package does not support decrypting pkcs8
	// keys
	var (
		rsaKey *rsa.PrivateKey
		ok     bool
	)
	if *snowflakePrivateKeyFile != "" {
		key, err := pemutil.Read(
			*snowflakePrivateKeyFile,
			// Can pass the passcode even if it's not set (indicating the key is
			// not encrypted), decryption will just be skipped in that case
			pemutil.WithPassword([]byte(*snowflakePrivateKeyPasscode)),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed parsing private key!")
		}

		rsaKey, ok = key.(*rsa.PrivateKey)
		if !ok {
			log.Fatal().Msg("Type assertion to *rsa.PrivateKey failed!")
		}
	}

	cfg := gosnowflake.Config{
		Account:       *snowflakeAccount,
		User:          *snowflakeUser,
		Database:      *snowflakeDatabase,
		Schema:        *snowflakeSchema,
		Role:          *snowflakeRole,
		Host:          *snowflakeHost,
		Port:          *snowflakePort,
		Protocol:      *snowflakeProtocol,
		MaxRetryCount: *snowflakeMaxRetryCount,
	}

	// Now add either private key, password, or external browser depending on flags
	if *snowflakePassword != "" {
		cfg.Authenticator = gosnowflake.AuthTypeSnowflake
		cfg.Password = *snowflakePassword
	} else if *snowflakePrivateKeyFile != "" {
		cfg.Authenticator = gosnowflake.AuthTypeJwt
		cfg.PrivateKey = rsaKey
	} else if *snowflakeAuthenticator == "externalbrowser" {
		cfg.Authenticator = gosnowflake.AuthTypeExternalBrowser
	} else {
		log.Fatal().
			Str("authenticator", *snowflakeAuthenticator).
			Msg("Invalid authenticator")
	}

	dsn, err := gosnowflake.DSN(&cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create DSN from config")
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatal().Err(err).Msg("Error connecting to snowflake")
	}
	defer db.Close()

	// Now can use all standard *sql.DB methods to query snowflake
	query := `SELECT current_timestamp() as TIME, current_user() as USER, current_role() as ROLE;`
	log.Debug().Str("query", query).Msg("Querying snowflake")
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed querying snowflake!")
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal().Err(err).Msg("Error getting column names!")
	}

	// Setup CSV writer
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	// Write header
	if err := w.Write(columns); err != nil {
		log.Fatal().Err(err).Msg("Error writing CSV header!")
	}

	// Create a slice of interface{} to store the row values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through rows
	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal().Err(err).Msg("Error scanning rows!")
		}

		// Convert values to strings
		stringValues := make([]string, len(columns))
		for i, v := range values {
			stringValues[i] = fmt.Sprint(v)
		}

		// Write CSV row
		if err := w.Write(stringValues); err != nil {
			log.Fatal().Err(err).Msg("Error writing CSV row!")
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		log.Fatal().Err(err).Msg("Error calling rows.Err!")
	}

	if err := rows.Close(); err != nil {
		log.Fatal().Err(err).Msg("Error calling rows.Close!")
	}

	// Log our counts and exit
	log.Debug().
		Int("rows", rowCount).
		Msg("Successfully pulled results from snowflake")
}

func signalHandlerContext(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		// The signal handler is removed after the first signal is processed or
		// the context is cancelled, which causes the program to revert to the
		// default signal handling behavior of terminating the program
		// immediately. The next signal received will therefore cause immediate
		// termination. If this causes too many accidental terminations, we
		// could leave the signal handler in place and rely on SIGTERM/SIGKILL
		// for forcible terminations instead.
		defer signal.Stop(sigs)

		select {
		case sig := <-sigs:
			log.Warn().
				Str("signal", sig.String()).
				Msg("Caught signal")
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}
