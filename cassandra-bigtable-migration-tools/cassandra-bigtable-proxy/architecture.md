# Cassandra-to-Bigtable Proxy Architecture

This document describes the high-level architecture of the Cassandra-to-Bigtable proxy, which translates Cassandra Query Language (CQL) requests into Google Cloud Bigtable operations.

## Overview

The proxy acts as an intermediary between Cassandra clients and Bigtable. It implements the Cassandra Native Protocol, allowing existing applications to interact with Bigtable using CQL drivers. The proxy handles the translation of CQL queries into Bigtable SQL or Bigtable Mutation API calls.

## System Components

The project is structured into several key layers:

### 1. Server & Protocol Layer
- **`proxy.go` (root)**: The entry point that initializes the proxy.
- **`third_party/datastax/proxy`**: Contains the core networking logic, session management, and request handling. It uses the `proxycore` library for Cassandra protocol frame handling.
- **`third_party/datastax/proxy/proxy.go`**: The `Proxy` and `client` structs manage the lifecycle of client connections and route incoming CQL frames (e.g., `QUERY`, `PREPARE`, `EXECUTE`, `BATCH`) to the translation and execution layers.

### 2. Parsing Layer
- **`third_party/cqlparser`**: An ANTLR-based CQL parser that converts raw CQL strings into a parse tree.
- **`parser/parser.go`**: Provides a `ProxyCqlParser` wrapper to facilitate navigation and extraction of information from the parse tree.

### 3. Translation Layer (`translators/`)
This layer is responsible for converting CQL into intermediate, structured query types.
- **`TranslatorManager`**: Orchestrates the translation process by routing queries to specific translators based on the `QueryType` (SELECT, INSERT, UPDATE, etc.).
- **Specific Translators**: (e.g., `select_translator`, `insert_translator`) handle the logic of mapping CQL clauses to Bigtable equivalents. For `SELECT` queries, this involves generating Bigtable SQL. For mutations, it involves preparing mutation data.
- **`IPreparedQuery`**: The output of the translation phase, representing a query that has been parsed and mapped but might still have unbound parameters.

### 4. Binding Phase
- **`BindQuery`**: Before execution, `TranslatorManager.BindQuery` takes a `IPreparedQuery` and client-provided values to produce an `IExecutableQuery`. This step handles parameter substitution and type conversion.

### 5. Execution Layer (`executors/`)
- **`QueryExecutorManager`**: Routes an `IExecutableQuery` to the appropriate executor.
- **`BigtableExecutor`**: The primary executor for most DML and DDL queries. it uses the `BigtableAdapter` to interact with the Bigtable API.
- **`DescribeExecutor`**: Handles `DESCRIBE` queries by fetching schema information from the metadata cache.
- **`UseExecutor`**: Handles `USE <keyspace>` queries by updating the session state.
- **`SelectSystemTableExecutor`**: Routes queries for Cassandra system tables to an in-memory engine.

### 6. Bigtable Adapter (`bigtable/`)
- **`BigtableAdapter`**: A high-level wrapper around the official Google Cloud Bigtable Go client. It handles:
    - Executing Bigtable SQL queries.
    - Applying single or bulk mutations.
    - Table administration (Create, Drop, Truncate).

### 7. Metadata & Schema Mapping (`metadata/`)
- **`MetadataStore`**: Manages the mapping between Cassandra tables/columns and Bigtable tables/families/qualifiers.
- **Schema Mapping Table**: The proxy stores its schema definitions in a dedicated Bigtable table (often named `schema_mapping`). This table records column types, primary key roles, and ordering.
- **Caching**: Schema information is cached in memory for performance and refreshed during DDL operations.

### 8. In-Memory Engine (`mem_table/`)
- **`InMemEngine`**: A basic in-memory query engine used to respond to queries against Cassandra system tables (e.g., `system.local`, `system.peers`), which are required by many Cassandra drivers to discover cluster topology.

## Data Flow

### Ad-hoc Query (QUERY)
1.  Client sends a `QUERY` frame.
2.  `Proxy` receives the frame and parses the query type.
3.  `TranslatorManager` translates the CQL into an `IPreparedQuery`.
4.  `TranslatorManager` binds any provided parameters to create an `IExecutableQuery`.
5.  `QueryExecutorManager` selects an executor (e.g., `BigtableExecutor`).
6.  Executor runs the query against Bigtable and receives results.
7.  Results are formatted into a Cassandra `RESULT` frame and sent back to the client.

### Prepared Query (PREPARE & EXECUTE)
1.  **PREPARE**:
    - Client sends a `PREPARE` frame.
    - `Proxy` translates the query and caches the `IPreparedQuery` with a unique ID.
    - Proxy returns a `PREPARED` result frame containing the ID and metadata about parameters.
2.  **EXECUTE**:
    - Client sends an `EXECUTE` frame with the query ID and parameter values.
    - `Proxy` retrieves the `IPreparedQuery` from the cache.
    - `TranslatorManager` binds the values to create an `IExecutableQuery`.
    - `QueryExecutorManager` executes the query and returns the results.

### Batch Query (BATCH)
1.  Client sends a `BATCH` frame containing multiple mutations.
2.  `Proxy` iterates through the mutations, preparing and binding each one.
3.  Mutations for the same table are grouped.
4.  `BigtableAdapter` uses `ApplyBulkMutation` to execute them efficiently.

## Observability and Monitoring

- **`otel/`**: The proxy integrates with OpenTelemetry for tracing and metrics. It records spans for query handling, translation, and Bigtable execution, providing visibility into latency and errors.
- **Logging**: Uses `uber-go/zap` for structured, high-performance logging.

## Response Handling

- **`responsehandler/`**: Contains utilities for converting Bigtable results (from the SQL API or Row API) back into Cassandra protocol frames. This includes handling data type conversions and row formatting.

## Configuration

- **`config.yaml`**: The primary configuration file for the proxy. It defines:
    - Listen address and port.
    - Bigtable project, instance, and keyspace mappings.
    - Security settings (TLS, authentication).
    - Logging levels and OpenTelemetry exporters.
    - Schema mapping table name.

## CQL Data Type Storage in Bigtable

The proxy maps Cassandra's data model to Bigtable's cell-based storage using several strategies depending on the CQL type and the column's role (Primary Key vs. Regular Column).

### 1. Primary Keys (Row Key)
Primary keys (partition and clustering keys) are concatenated to form the Bigtable Row Key.
- **Encoding**: Uses an "Ordered Code" encoding (see `translators/common/orderedcode.go`) to ensure that the byte-order of the Bigtable row keys matches the logical CQL ordering (especially important for clustering columns).
- **String/Blob**: Encoded using Ordered Code but with the trailing delimiter removed to allow for prefix scans.
- **Integers**: Encoded to preserve numerical order. Supports both Big-Endian (for non-negative values) and Ordered Code (for full range support).
- **Timestamp**: Converted to Unix microseconds and encoded as an ordered 64-bit integer.
- **Delimiter**: Components of a composite primary key are separated by a specific 2-byte delimiter (`\x00\x01`).

### 2. Regular Columns (Cell Values)
For regular (non-primary key) columns, values are generally stored as the cell's value.
- **Encoding**: Most scalar types are encoded using the Cassandra Native Protocol's Big-Endian format (Protocol Version 4).
- **Scalars**:
    - `text`, `varchar`, `ascii`: Raw UTF-8 string bytes.
    - `int`, `tinyint`, `smallint`: Encoded as 32-bit or 64-bit Big-Endian integers.
    - `bigint`, `counter`: Encoded as 64-bit Big-Endian integers.
    - `boolean`: Stored as a 64-bit Big-Endian integer (1 for true, 0 for false).
    - `float`, `double`: Standard IEEE 754 Big-Endian floating-point encoding.
    - `timestamp`: 64-bit Big-Endian integer representing milliseconds since epoch.
    - `blob`: Raw bytes.
    - `uuid`, `timeuuid`: 16-byte raw representation.

### 3. Collection Types
Collections are mapped to multiple Bigtable cells within the same row and column family, using the **Column Qualifier** to store element information.
- **Sets**: Each element is a separate cell. The **Column Qualifier** is the string representation of the element value (e.g., `"123"` for an int, `"true"` for a bool). The cell value is empty.
- **Maps**: Each key-value pair is a separate cell. The **Column Qualifier** is the string representation of the map key. The cell value is the encoded map value (using the same Big-Endian encoding as regular columns).
- **Lists**: To maintain order while allowing efficient appends/prepends, each element is stored in a cell where the **Column Qualifier** is a 12-byte custom-encoded value. It consists of an 8-byte Big-Endian timestamp (milliseconds) followed by a 4-byte Big-Endian "nanoseconds" offset to ensure uniqueness even for elements added at the same millisecond.

### 4. Special Types
- **Counters**: Use Bigtable's native counter support (64-bit increments).

## Schema Mapping Strategy

- **Keyspace**: Each Cassandra keyspace is mapped to a Bigtable Instance (configured in `config.yaml`).
- **Table**: Each Cassandra table is mapped to a unique Bigtable table.
- **Column Families**: By default, all regular columns are mapped to a single Bigtable column family (e.g., `cf`).
- **Primary Keys**: Cassandra's Partition and Clustering keys are combined into the Bigtable Row Key. The `MetadataStore` tracks the order and types of these components to ensure correct row key generation and query translation.
- **Types**: Cassandra types (e.g., `text`, `int`, `uuid`, `timestamp`) are mapped to their Bigtable SQL equivalents or serialized to bytes for storage in cells.
