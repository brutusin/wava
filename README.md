#org.brutusin:wava [![Build Status](https://api.travis-ci.org/brutusin/wava.svg?branch=master)](https://travis-ci.org/brutusin/wava) [![Maven Central Latest Version](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/)

**[W]hen [AVA]ilable linux scheduler**: A RSS-memory-aware scheduler for Linux batch processes. 

`wava` allows to enqueue batch jobs (submitted with a maximum physical memory usage promise) to be executed when enough physical memory ([RSS](https://en.wikipedia.org/wiki/Resident_set_size)) is available in the system.

<p align="center">
    <img src="https://github.com/brutusin/wava/raw/master/img/wava-menu.gif" alt="wava shiny logo">
</p>

This scheduler has been created originally to enqueue a high number of jobs in machines with a large amount of RAM, running concurrently the largest number of them but avoiding memory paging and swapping in order to not penalize the performance of other services running in the system.

<p align="center">
    <img src="https://github.com/brutusin/wava/raw/master/img/wava-while.gif" alt="wava example">
    <img src="https://github.com/brutusin/wava/raw/master/img/wava-ls.gif" alt="wava example">
</p>

(... to be continued)

## Requirements
`$JAVA_HOME` environment variable set pointing to a JRE 8+

## Installation
### 1. Download latest version:
```sh
wget -O wava-latest-dist.zip "https://repository.sonatype.org/service/local/artifact/maven/content?r=central-proxy&g=org.brutusin&a=wava&c=dist&e=zip&v=LATEST"
```
### 2. Decompress the distribution zip
```sh
unzip wava-latest-dist.zip -d .
```
### 3. Export extract folder path into the `$WAVA_HOME` variable
```sh
export WAVA_HOME=`pwd`/`unzip -Z -1 wava-latest-dist.zip | head -n 1 | sed 's#/*$##;s#^/*##'`
```
### 3. Set folder permissions to `755`
```sh
sudo chmod -R 755 $WAVA_HOME
```
### 4. Run to verify installation and generate default configuration file
```sh
$WAVA_HOME/bin/wava
```

## Configuration
Configuration is set in file `$WAVA_HOME/cfg/wava.json`.
### Default configuration
At startup, the `wava` process writes to disk the default configuration the following way:
- To `$WAVA_HOME/cfg/wava.json` in case this file doesn't exist
- To `$WAVA_HOME/cfg/wava.json.default` always


```javascript
{
  "uICfg" : {
    "ansiColors" : true
  },
  "schedulerCfg" : {
    "promiseHandlerClassName" : "org.brutusin.wava.core.plug.impl.StrictPromiseHandler",
    "pollingSecs" : 5,
    "maxTotalRSSBytes" : -1,
    "maxJobRSSBytes" : -1,
    "commandTTLCacheSecs" : 2,
    "sigKillDelaySecs" : 5
  },
  "processCfg" : {
    "nicenessRange" : [ -20, 19 ],
    "cpuAfinity" : "0-63"
  },
  "groupCfg" : {
    "dynamicGroupIdleSeconds" : 10,
    "predefinedGroups" : [ {
      "name" : "high",
      "priority" : -10,
      "timeToIdleSeconds" : -1
    }, {
      "name" : "low",
      "priority" : 10,
      "timeToIdleSeconds" : -1
    } ]
  }
}
```

## Running modes

## Priority groups

## Support bugs and requests
https://github.com/brutusin/linux-scheduler/issues

## Authors

- Ignacio del Valle Alles (<https://github.com/idelvall/>)

Contributions are always welcome and greatly appreciated!

##License
Apache License, Version 2.0
