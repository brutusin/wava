#org.brutusin:wava [![Build Status](https://api.travis-ci.org/brutusin/wava.svg?branch=master)](https://travis-ci.org/brutusin/wava) [![Maven Central Latest Version](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/)

**[W]hen [AVA]ilable linux scheduler**: A RSS-memory-aware scheduler for Linux batch processes. 

`wava` allows to enqueue batch jobs (submitted with a maximum physical memory usage promise) to be executed when enough physical memory ([RSS](https://en.wikipedia.org/wiki/Resident_set_size)) is available in the system.

![wava menu](https://github.com/brutusin/wava/raw/master/img/wava-menu.gif)

## Overview

This scheduler has been created originally to enqueue a high number of long-running jobs in machines with a large amount of RAM, and run as most of them concurrently, avoiding memory paging and swapping in order to not penalize the performance of other services running in the system.

The scheduler runs as a centralized process (`wava -s`) and the processes of rest of the commands communicate with the scheduler via named pipes. In particular job submissions are performed by separate peer processes (`wava -r`) that serve as lightweight placeholders of the real jobs executed by the scheduler. Peer and job processes have their lifecycle bound to each other. If one dies the other dies too.

The scheduler pipes job stderr and stdout to the respective streams of their peers. Aditionally it pipes scheduler events to the peer stderr unless an (event)[#Events] file has been specified in submission (`wava -r -e`).

![wava example 1](https://github.com/brutusin/wava/raw/master/img/wava-example1.gif)
*Running `ls` with a promise of max memory of 100 B*

![wava example 2](https://github.com/brutusin/wava/raw/master/img/wava-example2.gif)
*Running a loop that prints the date each 1 second with an excesive promise of max memory of 100000000 B that makes the command to be  temporary queued*

(... to be continued)

## Requirements
`$JAVA_HOME` environment variable set pointing to a JRE 8+

## Installation
### 1. Download latest version:
```sh
export WAVA_VERSION=`wget -O - -o /dev/null https://repo1.maven.org/maven2/org/brutusin/wava/maven-metadata.xml | grep '<latest>' | grep -Eow '[0-9\.]*'`
wget -O wava-latest-dist.zip "https://repository.sonatype.org/service/local/artifact/maven/content?r=central-proxy&g=org.brutusin&a=wava&c=dist&e=zip&v=$WAVA_VERSION"
```
*This `$WAVA_VERSION` variable has been created only for installation purposes and doesn't need to be persisted for future sessions*
### 2. Decompress the distribution zip:
```sh
unzip wava-latest-dist.zip -d .
```
### 3. Set appropriate file permissions:
```sh
chmod -R 555 wava-$WAVA_VERSION
chmod 777 wava-$WAVA_VERSION/tmp
```

### 5. Move and create symbolic links:
```sh
sudo mkdir -p /opt/wava
sudo mv wava-$WAVA_VERSION /opt/wava
sudo ln -sf /opt/wava/wava-$WAVA_VERSION/bin/wava /usr/bin/wava
```

### 6. Run to verify installation and generate default configuration file:
```sh
wava
```

### 7. Run on startup
Optinally, create a service to run the following command at startup by the "root" user: `wava -s`. Details are not given here since it varies depending on the Linux distribution.

## Configuration
Configuration is set in file: `$WAVA_HOME/cfg/wava.json`.
### Default configuration

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
## Priority and groups
All submitted jobs belong to a group that determines their priority 

## Events

## Support bugs and requests
https://github.com/brutusin/linux-scheduler/issues

## Authors

- Ignacio del Valle Alles (<https://github.com/idelvall/>)

Contributions are always welcome and greatly appreciated!

##License
Apache License, Version 2.0
