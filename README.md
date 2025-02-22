# snowcat

A very basic Snowflake query CLI.

## Usage

```
A very basic Snowflake query CLI.

Usage of snowcat:
  -query string
        SQL query to execute (default "SELECT current_timestamp() as TIME, current_user() as USER, current_role() as ROLE;")
  -snowflake.account string
        Account name for snowflake. Account name is not the username, see https://docs.snowflake.com/en/user-guide/admin-account-identifier for more details
  -snowflake.authenticator string
        Authenticator type for snowflake (one of: externalbrowser)
  -snowflake.database string
        Database name for snowflake
  -snowflake.host string
        Host name for snowflake (default: {account}.snowflakecomputing.com)
  -snowflake.max.retry.count int
        Specifies maximum number of subsequent retries with backoff. Use -1 for no retries, as 0 means use the default. (default 7)
  -snowflake.password string
        Password for snowflake. Cannot be used in conjunction with snowflake.private.key.file
  -snowflake.port int
        Port for snowflake connection (default 443)
  -snowflake.private.key.file string
        Location of private key file used to authenticate with snowflake, pkcs8 in PEM format. Cannot be used in conjunction with snowflake.password
  -snowflake.private.key.passcode string
        Passcode for encrypted private key (not necessary if key is not encrypted)
  -snowflake.protocol string
        Protocol for snowflake connection (http or https) (default "https")
  -snowflake.role string
        Role for snowflake
  -snowflake.schema string
        Schema name for snowflake
  -snowflake.user string
        Username for snowflake
```

## Example usage

Connect using externalbrowser auth:

```
go run . -snowflake.account gaga.us-east-1 -snowflake.user lady@gaga.com -snowflake.authenticator externalbrowser
```

Connect to fakesnow:

```
go run . -snowflake.host localhost -snowflake.account fakesnow -snowflake.user fake -snowflake.password snow -snowflake.port 8000 -snowflake.protocol http -snowflake.max.retry.count 1
```
