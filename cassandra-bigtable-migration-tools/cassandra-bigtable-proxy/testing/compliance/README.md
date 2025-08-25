# Compliance Tests

This set of "Compliance" tests are intended to be executed against either the
Proxy or a true Cassandra instance. Running against both targets ensures that
Proxy behavior is consistent with Cassandra behavior. These tests are also
treated as Integration Tests, so tests that aren't concerned with compliance can
be added here too.

## Running

1. First, start your Cassandra or Proxy instance.
2. `cd` into `testing/compliance/`
3. To test the Proxy run: `go test .` or `COMPLIANCE_TEST_TARGET=proxy go test .`
4. To test Cassandra run: `COMPLIANCE_TEST_TARGET=cassandra go test .`

## Contributing

All tests should be run in parallel to ensure correctness and prompt test
running. Please add `t.Parallel()` at the start of all new tests to enable
parallelism.