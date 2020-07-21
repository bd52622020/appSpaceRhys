#!/usr/bin/env bash

mkdir -p ../../../logs/kafka_success
mkdir -p ../../../logs/kafka_failure

consumer(){
	until python3 ./transcript.py $1 transcripts ../../../ ; do
    		echo "${1} consumer crashed with exit code $?.  Restarting.." >&2
    		sleep 1
	done
}
producer(){
	until python3 ./input.py $1 $2 ../../../; do
		echo "${1} producer crashed with exit code $?. Restarting.." >&2
		sleep 1
	done
}

producer $1 $2 &
consumer $1 &
wait
