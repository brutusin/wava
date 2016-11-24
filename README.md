## org.brutusin:wava [![Build Status](https://api.travis-ci.org/brutusin/wava.svg?branch=master)](https://travis-ci.org/brutusin/wava) [![Maven Central Latest Version](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava-root/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava-root/)

`wava` is a simple Linux command-line tool that allows to enqueue batch jobs (submitted with a maximum [RSS](https://en.wikipedia.org/wiki/Resident_set_size) usage promise) to be executed when enough physical memory is available in the system, and once running, manages their process nicenesses.

![wava menu](https://github.com/brutusin/wava/raw/master/img/wava-menu.gif)

**Table of contents**
- [Top](#orgbrutusinwava--)
- [Overview](#overview)
  * [Memory promises](#memory-promises)
- [Priority and groups](#priority-and-groups)
  * [Job order](#job-order)
  * [Niceness](#niceness)
- [Events](#events)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Support bugs and requests](#support-bugs-and-requests)
- [Authors](#authors)
- [License](#license)


## Overview

This scheduler is specially suited for enqueuing a high number of heterogeneous (in terms of memory demands) long-running jobs and run as many of them as possible concurrently without exhausting physical memory (avoiding virtual memory paging and swapping) in order to not penalize the performance of other services running in the system.

The scheduler runs as a centralized process (`wava -s`) and all interaction with it is performed by separated client processes started when invoking the other command options.  

In particular job submissions are performed by separate peer processes (`wava -r`) that serve as lightweight placeholders of the real jobs executed by the scheduler. Peer and job processes have their lifecycle bound to each other. If one dies the other dies too.

![wava menu](https://github.com/brutusin/wava/raw/master/img/process-diagram.png)

The scheduler pipes job `stderr` and `stdout` to the respective streams of their peers. Additionally, it pipes scheduler [events](#events) to the peer `stderr` unless an event file has been specified in submission (`wava -r -e <file>`).

### Memory promises
Each job is submitted with a promise of maximum RSS usage, and enqueued according to specific [ordering](#job-order) rules. The scheduler keeps track of these needs in order to guarantee that the set of running jobs never exceed a ([configurable](#configuration-description)) total amount of physical memory.

Since promises might be wrong, the scheduler periodically verifies them for the running jobs, and in case of failing, an action is performed (configurable implementation of [`PromiseHandler`](src/main/java/org/brutusin/wava/core/plug/PromiseHandler.java)); by default: killing the job process tree.


![wava example 1](https://github.com/brutusin/wava/raw/master/img/wava-example1.gif)
*Running `ls` with a promise of max memory of 100 B. The duration is of the job is too short for the scheduler detecting that it promised memory is too low*

![wava example 2](https://github.com/brutusin/wava/raw/master/img/wava-example2.gif)
*Running a loop that prints the date each 1 second with an excesive promise of max memory of 100 MB that makes the command to be  temporary queued. Observe also that this command runs untils user 'nacho' cancels it, returning a non-zero return code*

## Priority and groups

All submitted jobs belong to a priority group that determines their global ordering, used for positioning in the queue and assigning a process niceness when running.

Besides `priority` groups also have a `timeToIdleSeconds` property. This is the time elapsed between the last job finishes and the group is removed. If this value is set to `-1`, the group is eternal.

Jobs that do not specify a group at submit time are assigned to the `default` group (`priority=0`, `timeToIdleSeconds=-1`).

Jobs that specify a non-existing group create at submit-time a *dynamic* group (`priority=0`, `timeToIdleSeconds` specified at [configuration](#configuration-description)).

![wava group listing](https://github.com/brutusin/wava/raw/master/img/wava-groups.gif)
*Sample output of command `wava -g -l` for querying groups*

### Job order

Jobs are ordered by the following rules:
- First by group priority (lower value means higher priority)
- Then by group id (incremental). In case of same priority, jobs of the oldest group go first.
- Finally, by job id (incremental). For jobs inside the same group, FIFO ordering.

![wava job listing](https://github.com/brutusin/wava/raw/master/img/wava-jobs.gif)
*Sample output of command `wava -j` for querying jobs (white: running, yellow: queued). The scheduler has been configured to accept a maximum total RSS of 100000 B*

### Niceness
The scheduler sets the niceness of the job processes according to their global ordering within the working niceness range. The concrete strategy is determined by the [`NicenessHandler`](src/main/java/org/brutusin/wava/core/plug/NicenessHandler.java) implementation used (set the [configuration](#configuration-description)). 

## Events
Besides `stderr` and `stdout`, the scheduler process maintains a dedicated channel (named pipe) for communicating events to client processes. These events are serialized in the form:
```
${time-millis}:${event-type}[:${event-value}]
```
For peer processes these events are output to `stderr` after being formatted as `[wava] [date] [${event-type}:${event-value}]` unless a file is specified (`wava -r -e <file>`) for redirecting them.

Event type ([`Events.java`](wava-client/src/main/java/org/brutusin/wava/io/Event.java)) | Valued | Description
-------------- | --- | -----
`id`               | yes | Id assigned to the job.
`queued`           | yes | Position in the queue, if the job is queued.
`priority`         | yes | Piority of the job, given by its group. 
`running`          | yes | Root pId of the job process when started.  
`niceness`         | yes | Niceness set to the job process.   
`cancelled`        | yes | User cancelling the job. 
`ping`             | no  | Send periodically to detect stale peers.
`exceed_allowed`   | yes | Memory promise failed but execution allowed
`exceed_disallowed`| yes | Memory promise failed and execution disallowed 
`exceed_global`    | yes | Memory promise too high (more than [config](#configuration-description) param `maxJobRSSBytes`)
`exceed_tree      `| yes |  
`shutdown`         | yes | Memory promise failed and execution disallowed 
`maxrss`           | yes | Max RSS used by a finished job process (and their decendents).
`error`            | yes | To send information about an error.
`retcode`          | yes | Return code for the client process to use.
`deadlock_relaunch`| yes | 
`deadlock_stop`    | yes |

## Job hierarchy
### Deadlock prevention

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
unzip -o wava-latest-dist.zip -d .
```
### 3. Set appropriate file permissions:
```sh
chmod -R 555 wava-$WAVA_VERSION
```

### 4. Move and create symbolic links:
```sh
sudo mkdir -p /opt/wava
sudo mv wava-$WAVA_VERSION /opt/wava
sudo ln -sf /opt/wava/wava-$WAVA_VERSION/bin/wava /usr/bin/wava
```

### 5. Run to verify installation and generate default configuration file:
```sh
wava
```

### 6. Run on startup
Optinally, create a service to run the following command at startup by the "root" user: `wava -s`. Details are not given here since it varies depending on the Linux distribution.

## Configuration
Configuration is set in file: `$WAVA_HOME/cfg/wava.json`. Environment variables can be used in this file.
### Default configuration

```javascript
{
  "uICfg" : {
    "ansiColors" : true,
    "sIMemoryUnits" : true
  },
  "schedulerCfg" : {
    "promiseHandlerClassName" : "org.brutusin.wava.core.plug.impl.StrictPromiseHandler",
    "nicenessHandlerClassName" : "org.brutusin.wava.core.plug.impl.HomogeneusSpreadNicenessHandler",
    "pollingSecs" : 5,
    "maxTotalRSSBytes" : -1,
    "maxJobRSSBytes" : -1,
    "commandTTLCacheSecs" : 2,
    "sigKillDelaySecs" : 5
  },
  "processCfg" : {
    "nicenessRange" : [ -20, 19 ],
    "cpuAfinity" : "$DEFAULT_CPU_AFINITY"
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
### Configuration description
Property                               | Description
--------                               | -----------
`uICfg.ansiColors`                     | Use ANSI escape code sequences to highlight UI.
`uICfg.sIMemoryUnits`                  | Use units from the International System for output memory values. `true`: kB based, `false`:[KiB](https://en.wikipedia.org/wiki/Kibibyte) based
`schedulerCfg.promiseHandlerClassName` | FQN of the [`PromiseHandler`](src/main/java/org/brutusin/wava/core/plug/PromiseHandler.java) implementation (see [`impl`](https://github.com/brutusin/wava/tree/master/src/main/java/org/brutusin/wava/core/plug/impl) package) to use.
`schedulerCfg.nicenessHandlerClassName` | FQN of the [`NicenessHandler`](src/main/java/org/brutusin/wava/core/plug/NicenessHandler.java) implementation (see [`impl`](https://github.com/brutusin/wava/tree/master/src/main/java/org/brutusin/wava/core/plug/impl) package) to use.
`schedulerCfg.pollingSecs`             | Polling time interval for promises verification and ping events.
`schedulerCfg.maxTotalRSSBytes`        | Maximum amount of physical memory permitted for all jobs. If `-1` the total amount of physical memory is considered.
`schedulerCfg.maxJobRSSBytes`          | Maximum permitted RSS promise for a job. If `-1` there is no limit.
`schedulerCfg.commandTTLCacheSecs`     | Cache TTL in seconds, for some commands used to query information to the system.
`schedulerCfg.sigKillDelaySecs`        | Seconds between SIGTERM and SIGKILL signals send in job cancellation.
`processCfg.nicenessRange`             | Minimum (most favorable) and maximum (less favorable) niceness to be assigned to a job process tree
`processCfg.cpuAfinity`                | CPU affinity to be set to the job processes. In a format supported by the `-c` parameter of [taskset](http://linuxcommand.org/man_pages/taskset1.html).
`groupCfg.dynamicGroupIdleSeconds`     | Idle time for [dynamic groups](#priority-and-groups) in seconds.
`groupCfg.predefinedGroups`            | Set of groups to be available since startup.
## Support bugs and requests
https://github.com/brutusin/wava/issues

## Authors

- Ignacio del Valle Alles (<https://github.com/idelvall/>)

Contributions are always welcome and greatly appreciated!

##License
Apache License, Version 2.0
