#usr/bin/env bash

wget --recursive -l 1 --quota=$2 -R -A.html -e robots=off $1

for file in $1/*
do
	NEWNAME=$(basename $file .html)_$3.html
	cp $file $1/$NEWNAME
done
