# immudb performance test suite

This tool is the main immudb performance analysis tool with the goal of checking immudb performance
with various real-world workloads.

## Long vs short run

The test size is configurable. Short tests are meant to do a quick performance test on every immudb push to a master branch.
Long test must be executed before each release to ensure there are no performance regressions.

The default mode is to run a short performance test.

## Output

This tool produces a json output file with detailed information about the performance.
It contains timeline of various measurements throughout the test and summary.
If possible, the json file will also contain metadata gathered from the underlying system necessary for comparisons between different systems.

## Central storage for test results

Currently the results of performance tests are only attached to CI output.
It is planned to have a central place with all the results gathered over time.
