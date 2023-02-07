![Streamliner Logo](docs/images/streamliner_logo.png)

# User Documentation

See the [Docs Directory](docs/) for user docs.

# Developer Documentation

## Packaging Streamliner
Streamliner uses [Gradle](https://gradle.org/) as a dependency management and build tool.
The [Gradle Application Plugin](https://docs.gradle.org/current/userguide/application_plugin.html) attempts to make building packages for different operating systems easier.

### Creating Streamliner Package
1. Execute `./gradlew clean assemble`
2. Copy zip from `build/distributions/streamliner-<version>.zip` to intended install directory
3. Unzip streamliner

### Releasing

Install tools if required:

```shell script
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install --upgrade cloudsmith-cli
```

Login to Cloudsmith:

```shell script
$ cloudsmith token
```

Publish:

```shell script
$ build-support/publish-zip.sh
```

### Running Full Build

```shell script
./gradlew build
```

### Running Tests

Note: Some tests require Docker to be running.

```shell script
./gradlew test
```

