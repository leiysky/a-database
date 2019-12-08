# A-database

This is a simple database project for learning how to build an RDBMS.

## Build

You need to install `Golang` first, version `>1.10` is preferred.

Follow the command to build:
```shell
$ go build
```

## Run

There are some prepared SQLs for test.

First, start your server:
```shell
$ ./a-databse
```

`a-database` will start a HTTP server which listens `localhost:3399` by default.

You can use:
```shell
$ curl -X GET localhost:3399/ping
```
to test whether if the server is running.

If server is running, you can run SQL on it with:
```shell
$ cd examples
examples/ $ ./run-sql.sh show_table.sql
```

## TODO

For now `a-database` is just a crude database, which means there are many issues you can solve.

We'll publish some issues for contributors.

You can contact me with email. [leiysky@outlook.com](mailto:leiysky@outlook.com)
