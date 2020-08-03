# Music [![Build Status](https://travis-ci.org/sarthak-kakkar/music.svg?branch=master)](https://travis-ci.org/sarthak-kakkar/music)
Enriching records from LastFm's APIs using Kafka streams and producer. 

## Prerequisite
- Use `docker-compose`, present in the `bin` directory to setup the Kafka cluster.
- Other than that obtain an API key from LastFm and add it to the constant present in `MusicRecordsProducer.java`
- You can also use the scripts under bin folder to create topics.

## Running the application
The application can be bundled up, to create an executable jar, by running `./gradlew clean jar`, the jar can be found under `build/libs` directory.  

Console application can also be started by executing `./gradlew clean run`.

## Todo
Create a consumer for the `results` topic.