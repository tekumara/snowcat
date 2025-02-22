# snowcat

A very basic Snowflake query CLI.

Connect using externalbrowser auth:

```
go run . -snowflake.account gaga.us-east-1 -snowflake.user lady@gaga.com -snowflake.authenticator externalbrowser
```

Connect to fakesnow:

```
go run . -snowflake.host localhost -snowflake.account fakesnow -snowflake.user fake -snowflake.password snow -snowflake.port 8000 -snowflake.protocol http -snowflake.max.retry.count 1
```
