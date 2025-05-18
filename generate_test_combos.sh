#!/bin/bash

#Activate venv
source venv/bin/activate

#Generate small pseudo combos for testing
python main.py generate-pseudo-combos \
    -nw 10000 \
    -e 0.example.com,1.example.com,2.example.com \
    -d ":" \
    -nc 100000 \
    -o ./tests/Data/delim_colon_100000.txt
python main.py generate-pseudo-combos \
    -nw 10000 \
    -e 0.example.com,1.example.com,2.example.com \
    -d "|" \
    -nc 100000 \
    -o ./tests/Data/delim_pipe_100000.txt

#Create a tarball of the pseudo combos
cd ./tests/Data
tar -zcf \
    test_combos.tar.gz \
    delim_colon_100000.txt \
    delim_pipe_100000.txt

#Remove generated text files
rm delim_colon_100000.txt delim_pipe_100000.txt
