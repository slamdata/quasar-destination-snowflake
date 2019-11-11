# quasar-destination-snowflake [![Build Status](https://travis-ci.com/slamdata/quasar-destination-snowflake.svg?branch=master)](https://travis-ci.com/slamdata/quasar-destination-snowflake) [![Bintray](https://img.shields.io/bintray/v/slamdata-inc/maven-public/quasar-destination-snowflake.svg)](https://bintray.com/slamdata-inc/maven-public/quasar-destination-snowflake) [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.slamdata" %% "quasar-destination-snowflake" % <version>
```

## Configuration

```json
{
   accountName: String,
   warehouse: String,
   databaseName: String,
   user: String,
   password: String,
   schema: String
}
```

- `accountName` the name of the Snowflake account. `name` in `<name>.snowflakecomputing.com`
- `warehouse` the name of the warehouse to use
- `databaseName` the name of the database to use
- `user` the user name to login into Snowflake
- `password` the password to login into Snowflake

All fields are mandatory.
