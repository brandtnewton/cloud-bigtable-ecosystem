# Cassandra Proxy Java Client Tests

This directory contains a simple Maven-based Java test suite for the Cassandra Bigtable Proxy.

## Prerequisites

- Java 11 or higher
- Maven
- A running instance of the Cassandra Bigtable Proxy on `localhost:9042`

## Running the Tests

To run the tests, execute the following command in this directory:

```bash
mvn test
```

## Test Details

- **SimpleCassandraTest**: Performs a basic `CREATE KEYSPACE`, `CREATE TABLE`, `INSERT`, and `SELECT` operation to verify the proxy's functionality.
