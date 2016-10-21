#!/bin/bash
export WAVA_HOME="$(dirname "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )")"
java -jar $WAVA_HOME/lib/wava-1.4.2-SNAPSHOT-executable.jar "$@" < /dev/stdin 1>&1 2>&2
