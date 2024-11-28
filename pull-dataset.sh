#!/bin/bash

DATASET_REPO="/home/floppabox/f1/f1db"
if [ -d "$DATASET_REPO" ]; then
	cd "$DATASET_REPO" || exit
else
	echo "--Error: no directory as such--"
	exit 1
fi

echo "--Updating dataset--"
git pull origin main

if [ $? -eq 0 ]; then
	echo "--Dataset updated successfully--"
else
	echo "--Error: update failed--"
	exit 1
fi
