#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# Recall the two latter commands exit if any command returns nonzero,
# if an undefined variable is called, or if a command mid-pipeline fails,
# and iterates over elements separated by just newlines and tabs, not spaces.

# Handle FIMA NFIP Policies JSON file
# Connor P. Jackson - cpjackson@berkeley.edu
#
# Data are provided as a 40 GB JSON file (the API is flaky and times out for big queries
# or requests for data in CSV format). This script converts the JSON file into NDJSON,
# with one data entry per line. This format can then be streamed by R or python without
# reading the entire file into memory all at once, so we can convert to a CSV or read in
# a data.table or pd.DataFrame. Hopefully we only ever have to do this once. Just writing
# a script for documentation and future reference.
#
# Dependency: jq package for manipulating JSON: https://stedolan.github.io/jq/
#
# Initial format of the data:
# {"FimaNfipPolicies": [{entry1},{entry2},…,{entryN}
# Note the missing terminating ]} to close the data. That has to be dealt with.


# Steps:
# 1. Get access to the inner array of entries. The smart way to do that would probably be
#    to just strip the initial 21 characters from the top of the file, and add a closing ]
#    character, but I was not smart. So I called the jq function to convert the JSON in
#    place to NDJSON, which essentially just extracted that top level array.

cat fimanfippolicies.json | jq -c '.[]' > policies_ND_intermed.json

# Our file now looks like this:
# [{entry1},{entry2},…,{entryN}]

# I believe all we need to do is replace the comma separating each entry with a newline
# character, and remove the outer brackets. But to be sure we are following the NDJSON
# spec, we again use the jq command.

cat policies_ND_intermed.json | jq -c '.[]' > nfippolicies_NDJSON.json

# This should yield a file of the format
# {entry1}
# {entry2}
# …
# {entryN}
#
# Which we can stream in line by line to convert, without having to read in the entire file
# at once. Now we can just remove the intermediary file.

rm policies_ND_intermed.json
