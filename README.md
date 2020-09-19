![Streamliner Logo](customer-docs/images/streamliner_logo.png)

# User Documentation

See the [phData Streamliner User Documentation site](https://docs.customer.phdata.io/docs/streamliner/) for user docs.

## Packaging Streamliner
Streamliner uses [SBT](https://www.scala-sbt.org/) as a dependency management and build tool.  [SBT Native Packager](https://www.scala-sbt.org/sbt-native-packager/) attempts to make building packages for different operating systems easier.  Streamliner includes SBT Native Packager as a SBT plugin and should be used when packaging Streamliner.

### Creating Streamliner Package
1. Execute `sbt clean universal:packageBin`
2. Copy zip from `target/universal/streamliner-<version>.zip` to intended install directory
3. Unzip streamliner

## Running Integration Tests

```shell script
cd integration-tests/
./run-integration-tests.sh
```