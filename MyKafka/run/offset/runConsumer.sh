#!/bin/bash

FATJAR=$HOME/code/kafka/MyKafka/build/libs/MyKafka-all-1.0.jar
CONSUMER_CLASS=com.rueggerllc.kafka.offset.ConsumerApp

# arg0=topic arg1=group arg2=startingOffset
# startingOffset=0: beginning
# startingOffset=-1 end
# startingOffset=-2 manualCommit
# startingOffset=other seek to offset
java -cp $FATJAR $CONSUMER_CLASS $*
