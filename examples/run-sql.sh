#!/bin/bash

SQL_FILE=$1

function run_sql() {
    SQL_STRING=`cat $SQL_FILE | sed 's/\"/\\\"/g'`
    curl -X POST -d "{\"query\": \"$SQL_STRING\"}" localhost:3399/query
}

run_sql