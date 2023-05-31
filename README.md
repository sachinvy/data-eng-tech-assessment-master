## Task

This is a timeboxed task that requires you to develop a set of datapipelines / tasks that process pedestrian data form
the city of melbourne.
This task will require you to:

1. Load an external street data file and then send into a kafka topic (from the
   pedestrian-counting-system-monthly-counts-per-hour.json)
2. Read from the topic and enrich the message with the true location names (from the
   pedestrian-counting-system-sensor-locations.json file)
3. Finally write the enriched data into the iceberg database
4. Have at least one unit test
5. Instructions on how to build you pipelines

### Further information

- The language preference is Java and then other JVM
- Changes to the docker-compose are fine, including changing the container services or versions. Swapped out services
  should be something equivilant
- The current database is iceberg, in this case backed onto minio and presented via trino with a metastore catalog
- The data pipeline framework is apache beam, a flink server and worker are a park of the docker-compose
- There is a metabase container, this is optional to try and visualise data
- Don't over engineer, keep to the brief (there's plenty of room within that)

### Getting started

- Fork this repo as a starting point
- unzip the pedestrian.zip file in source-data
- Setup docker - see instructions below
- Copy or rename the .env.example to .env and populate the values for the containers
- Edit the conf files with your databases and credentials

```
conf/metastore/metastore-site.xml
conf/trino/catalog/hive.properties
conf/trino/catalog/iceberg.properties
```

- Run docker-compose

```
docker-compose up -d --force-recreate --remove-orphans
```

- Next access the [minio console](http://localhost:9090) and create a new bucket called "streets-data". You can also use
  the minio cli (mc) if preffered

Now you should have a workable stack to develop against.

### Next steps

- You will need to define and create the schema and tables, see example in the cli folder. This can be run by
  uncommenting the 'run-once' container trino-bootstrap3 in the docker-compose file
- You will need to create additional topics (the default could get used), they can be added in the "KAFKA_CREATE_TOPICS"
  variable for the kafka container
- You can upload and run your jar(s) to the flink master running [Here](http://localhost:8083)

### Submission

Once you have completed you solution share the github repo with your HR contact with instructions on how to build your
jars, do not share prebuilt jar files.

### Task files

1. Melbourne pedestrian data - newer version or different format can be
   downloaded [Here](https://discover.data.vic.gov.au/dataset/pedestrian-counting-system-monthly-counts-per-hour)
2. Location data - newer version or different format can be
   downloaded [Here](https://discover.data.vic.gov.au/dataset/pedestrian-counting-system-sensor-locations)

## Setup

### Docker Desktop

#### Windows:

1. Go to [Docker Desktop for Windows](https://docs.docker.com/docker-for-windows/install/) and download the executable
   file.
2. Run the executable file and follow the instructions to complete the installation.

#### Mac:

1. Go to [Docker Desktop for Mac](https://docs.docker.com/docker-for-mac/install/) and download the .dmg file.
2. Open the .dmg file and drag the Docker icon to the Applications folder.
3. Double-click the Docker icon in the Applications folder to launch Docker.

#### Linux:

1. Go to [Docker Desktop for Linux](https://docs.docker.com/engine/install/ubuntu/) and choose the appropriate
   installation method for your distribution.
2. Follow the instructions to complete the installation.

### Docker Compose

#### Windows, Mac, and Linux:

Docker compose is bundled with the most recent versions of docker-desktop. If you use docker-machine or something other
solution, you will need to use you OS package manager to install

### Podman

Alternative to docker.

#### Linux:

1. Go to the [Podman documentation](https://podman.io/getting-started/installation) and choose the appropriate
   installation method for your distribution.
2. Follow the instructions to complete the installation.

#### Mac:

1. Install [Homebrew](https://brew.sh/) if you haven't already.
2. Run `brew install podman`.

#### Windows:

1. Install [Windows Subsystem for Linux (WSL)](https://docs.microsoft.com/en-us/windows/wsl/install-win10) if you
   haven't already.
2. Install Podman using the WSL terminal. Instructions can be found on
   the [Podman documentation](https://podman.io/getting-started/installation#windows-subsystem-for-linux-wsl) page.

### Podman Compose

#### Linux:

1. Go to the [Podman Compose GitHub page](https://github.com/containers/podman-compose) and follow the instructions to
   download and install Podman Compose.

#### Mac and Windows:

1. Install Podman Compose using pip. Run `pip install podman-compose` in a terminal window.