#!/bin/bash

DATASET_REPO="/home/floppabox/f1/f1db"
GIT_DATASET="https://github.com/f1db/f1db.git"


if [ -d "$DATASET_REPO" ]; then
	cd "$DATASET_REPO" || exit
	echo "--Updating dataset--"
	git pull origin main
	if [ $? -eq 0 ]; then
		echo "--Dataset updated successfully--"
	else
		echo "--Error: update failed--"
		exit 1
	fi
else
	echo "--Cloning Dataset--"
	mkdir -p "$DATASET_REPO"
	git clone "$GIT_DATASET" "$DATASET_REPO"
	echo "--Dataset cloned successfully--"
fi
