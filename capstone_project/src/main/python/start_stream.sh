#!/bin/bash
mkdir -p ../../../logs/kafka_failure
mkdir -p ../../../logs/kafka_success/kafka_consumers
mkdir -p ../../../logs/kafka_success/kafka_producers

consumer(){
	until python3 /scripts/src/main/python/transcript.py $1 $2 "../../../logs" "${@:3}" ; do
    		echo "${1} consumer crashed with exit code $?.  Restarting.." >&2
    		sleep 1
	done
}
producer(){
	until python3 /scripts/src/main/python/input.py $1 $2 "../../../logs" "${@:3}" ; do
		echo "${1} producer crashed with exit code $?. Restarting.." >&2
		sleep 1
	done
}

producer $1 $2 "${@:4}" &
consumer $1 $3 "${@:4}" &
wait
