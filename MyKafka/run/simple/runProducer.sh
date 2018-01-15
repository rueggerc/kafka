#!/bin/bash

FATJAR=$HOME/code/kafka/MyKafka/build/libs/MyKafka-all-1.0.jar
PRODUCER_CLASS=com.rueggerllc.kafka.simple.ProducerApp

# java -cp $HOME/code/kafka/MyKafka/build/libs/MyKafka-all-1.0.jar com.rueggerllc.kafka.simple.ProducerApp $*
java -cp $FATJAR $PRODUCER_CLASS $*
