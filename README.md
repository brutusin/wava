#org.brutusin:wava [![Build Status](https://api.travis-ci.org/brutusin/wava.svg?branch=master)](https://travis-ci.org/brutusin/wava) [![Maven Central Latest Version](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/)

**[W]hen [AVA]ilable linux scheduler**: A RSS-memory-aware scheduler for Linux batch processes. 

`wava` allows to enqueue batch jobs (submitted with a maximum physical memory usage promise) to be executed when enough physical memory ([RSS](https://en.wikipedia.org/wiki/Resident_set_size)) is available in the system.

![wava menu](https://github.com/brutusin/wava/raw/master/img/wava-menu.gif)

This scheduler has been created originally to enqueue a high number of jobs in machines with a large amount of RAM, running concurrently the largest number of them but avoiding memory paging and swapping in order to not penalize the performance of other services running in the system.

![wava example 1](https://github.com/brutusin/wava/raw/master/img/wava-example1.gif)
*Running `ls` with a promise of max memory of 100 B*

![wava example 2](https://github.com/brutusin/wava/raw/master/img/wava-example2.gif)
*Running a loop that prints the date each 1 second with a promise of max memory of 100000000 B*

(... to be continued)

## Requirements
`$JAVA_HOME` environment variable set pointing to a JRE 8+

## Installation
### 1. Download latest version:
```sh
wget -O wava-latest-dist.zip "https://repository.sonatype.org/service/local/artifact/maven/content?r=central-proxy&g=org.brutusin&a=wava&c=dist&e=zip&v=LATEST"
```
### 2. Decompress the distribution zip:
```sh
unzip wava-latest-dist.zip -d .
```
### 3. Export extracted folder path into the `$WAVA_HOME` variable:
```sh
export WAVA_HOME=`pwd`/`unzip -Z -1 wava-latest-dist.zip | head -n 1 | awk -F "/" '{print $1}'`
```
*This `$WAVA_HOME` variable has been created only for installation purposes and doesn't need to be persisted for future sessions*
### 4. Set appropriate file permissions:
```sh
sudo chmod 777 $WAVA_HOME
sudo chmod -R 555 $WAVA_HOME/bin $WAVA_HOME/lib
```

### 5. Create symbolic link in `/usr/bin`:
```sh
sudo ln -s $WAVA_HOME/bin/wava /usr/bin/wava
```

### 6. Run to verify installation and generate default configuration file:
```sh
wava
```

## Configuration
Configuration is set in file: `$WAVA_HOME/cfg/wava.json`.
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
