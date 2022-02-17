![Streamliner Logo](customer-docs/images/streamliner_logo.png)

# User Documentation

See the [phData Streamliner User Documentation site](https://docs.customer.phdata.io/docs/streamliner/) for user docs.

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

### Running the Docker Container

Create a Docker image:
```bash
./gradlew installDist
docker build -t streamliner .
```

Copy the configuration and templates from the image:
```bash
docker cp $(docker create streamliner):/assets/conf conf
docker cp $(docker create streamliner):/assets/templates templates
```

Use Streamliner:

```bash
docker run streamliner --help                                                                                                                                                                                                                                                                                      phdata/streamliner - (actions)
     # --help   Show help message
     # ...
```

