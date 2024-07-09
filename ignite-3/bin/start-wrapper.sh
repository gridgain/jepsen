#!/bin/bash

set -e

nohup $1 1>>log/stdout.log 2>>log/stderr.log &
