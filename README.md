# quasar-destination-snowflake [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-snowflake" % <version>
```

## Configuration

```json
{
   accountName: String,
   warehouse: String,
   databaseName: String,
   user: String,
   password: String,
   schema: String,
   retryTransactionTimeoutMs: Optional<Number>,
   maxTransactionReattempts: Optional<Number>,
   sanitizeIdentifiers: Option<Boolean>
}
```

- `accountName` the name of the Snowflake account. `name` in `<name>.snowflakecomputing.com`
- `warehouse` the name of the warehouse to use
- `databaseName` the name of the database to use
- `user` the user name to login into Snowflake
- `password` the password to login into Snowflake
- `schema` the name of the schema to use
- `retryTransactionTimeoutMs` optional transaction retry timeout in milliseconds, default is 60000
- `maxTransactionReattempts` optional number of retries before giving up retrying, default is 10
- `sanitizeIdentifiers` optinal, defaults to `true`. When it's `true` identifiers (table name, schema, columns)
  are capitalized and all non-ASCII characters are replaced with `_`. If this is `false` the identifiers are
  quoted and `"` is replaced with `""`.

