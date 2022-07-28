# Github actions secrets

## PERF_TEST_RUNS_ON

This secret can be used to change the `runs-on` field for performance test suite.

Example value (quotes are necessary for correct json encoding and decoding):

```json
"runs-on": ["self-hosted", "perf-test"]
```

### PERF_TEST_AWS_xxx

If set, performance test results are uploaded into s3 after successful push workflow.

Following secrets are needed:

* `PERF_TEST_AWS_ACCESS_KEY_ID`
* `PERF_TEST_AWS_BUCKET_PREFIX` (i.e. `<bucket name>` or `<bucket_name>/some/prefix`)
* `PERF_TEST_AWS_REGION`
* `PERF_TEST_AWS_SECRET_ACCESS_KEY`
