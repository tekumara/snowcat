package main

// copied from https://github.com/DavidBrown-niche/gosnowflake-example/tree/0bcc7a5

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/peterbourgon/ff/v3"
	"github.com/snowflakedb/gosnowflake"
	"go.step.sm/crypto/pemutil"
	"go.uber.org/zap"
)

func main() {
	var (
		snowflakeAccount            = flag.String("snowflake.account", "", "Account name for snowflake. Account name is not the username, see https://docs.snowflake.com/en/user-guide/admin-account-identifier for more details")
		snowflakeDatabase           = flag.String("snowflake.database", "", "Database name for snowflake")
		snowflakeSchema             = flag.String("snowflake.schema", "", "Schema name for snowflake")
		snowflakeUser               = flag.String("snowflake.user", "", "Username for snowflake")
		snowflakePassword           = flag.String("snowflake.password", "", "Password for snowflake. Cannot be used in conjunction with snowflake.private.key.file")
		snowflakePrivateKeyFile     = flag.String("snowflake.private.key.file", "", "Location of private key file used to authenticate with snowflake, pkcs8 in PEM format. Cannot be used in conjunction with snowflake.password")
		snowflakePrivateKeyPasscode = flag.String("snowflake.private.key.passcode", "", "Passcode for encrypted private key (not necessary if key is not encrypted)")
	)

	if err := ff.Parse(flag.CommandLine, os.Args[1:], ff.WithEnvVars()); err != nil {
		log.Fatalf("Error parsing flags: %s", err)
	}

	// Create example logger:
	logger := zap.NewExample()

	defer logger.Sync()

	// Check flags
	if *snowflakeAccount == "" {
		logger.Fatal("Missing required flag snowflake.account")
	}
	if *snowflakeDatabase == "" {
		logger.Fatal("Missing required flag snowflake.database")
	}
	if *snowflakeSchema == "" {
		logger.Fatal("Missing required flag snowflake.schema")
	}
	if *snowflakeUser == "" {
		logger.Fatal("Missing required flag snowflake.user")
	}
	if *snowflakePassword == "" && *snowflakePrivateKeyFile == "" {
		logger.Fatal("Must provide exactly one of: snowflake.password OR snowflake.private.key.file, neither provided")
	}
	if *snowflakePassword != "" && *snowflakePrivateKeyFile != "" {
		logger.Fatal("Must provide exactly one of: snowflake.password OR snowflake.private.key.file, both provided")
	}

	// Create context that's cancelled when the program receives a SIGINT
	ctx := context.Background()
	ctx, cancel := signalHandlerContext(ctx, logger)
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
			logger.Fatal("Failed parsing private key!", zap.Error(err))
		}

		rsaKey, ok = key.(*rsa.PrivateKey)
		if !ok {
			logger.Fatal("Type assertion to *rsa.PrivateKey failed!")
		}
	}

	cfg := gosnowflake.Config{
		Account:  *snowflakeAccount,
		User:     *snowflakeUser,
		Database: *snowflakeDatabase,
		Schema:   *snowflakeSchema,
	}
	// Now add either private key or password depending on flags
	if *snowflakePassword != "" {
		cfg.Authenticator = gosnowflake.AuthTypeSnowflake
		cfg.Password = *snowflakePassword
	} else {
		cfg.Authenticator = gosnowflake.AuthTypeJwt
		cfg.PrivateKey = rsaKey
	}

	dsn, err := gosnowflake.DSN(&cfg)
	if err != nil {
		logger.Fatal("Failed to create DSN from config",
			zap.Error(err),
		)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		logger.Fatal("Error connecting to snowflake",
			zap.Error(err),
		)
	}
	defer db.Close()

	// Now can use all standard *sql.DB methods to query snowflake
	query := `
		SELECT 1;
	`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		logger.Fatal("Failed querying snowflake!", zap.Error(err))
	}
	// Do whatever you want with rows, for this example we'll just loop over
	// them and increment a count.
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		/*normally you would scan the rows here
		if err := rows.Scan(
			columns you want to scan
		); err != nil {
			logger.Fatal("Error scanning rows!", zap.Error(err))
		*/
	}

	if err := rows.Err(); err != nil {
		logger.Fatal("Error calling rows.Err!", zap.Error(err))
	}

	if err := rows.Close(); err != nil {
		logger.Fatal("Error calling rows.Close!", zap.Error(err))
	}

	// Log our count and exit
	logger.Sugar().Infof("Successfully pulled %d results from snowflake", count)
	return
}

func signalHandlerContext(ctx context.Context, logger *zap.Logger) (context.Context, func()) {
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
			logger.Info("Caught signal",
				zap.Stringer("signal", sig),
			)
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}
