## org.brutusin:wava [![Build Status](https://api.travis-ci.org/brutusin/wava.svg?branch=master)](https://travis-ci.org/brutusin/wava) [![Maven Central Latest Version](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava-root/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava-root/)

`wava` is a linux command line tool wich allows for multiple users to securely run batch processes, scheduded in a timely manner under contraints of allocated (resident) memory capacities.

![wava menu](https://github.com/brutusin/wava/raw/master/img/wava-menu.gif)

**Table of contents**
- [Top](#orgbrutusinwava--)
- [Overview](#overview)
- [Features](#features)
  * [Capacity guarantees](#capacity-guarantees)
  * [Security](#security)
  * [Resource-based scheduling](#resource-based-scheduling)
  * [Priority-based scheduling](#priority-based-scheduling)
- [Architecture](#architecture)
- [Priority and groups](#priority-and-groups)
  * [Job order](#job-order)
  * [Niceness](#niceness)
- [Events](#events)
- [Job hierarchy](#job-hierarchy)
  * [Blocked state](#blocked-state)
  * [Deadlock prevention](#deadlock-prevention)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Support bugs and requests](#support-bugs-and-requests)
- [Authors](#authors)
- [License](#license)


## Overview

`wava` scheduler is designed to run batch processes in a single Linux machine in an operator-friendly manner while imposing a memory limit across the overall job set (strict limits) and guaranteeing a minimum of available memory per job (soft limits).

Traditionally fixed-sized queues were used for enqueueing jobs, but when used over a heterogeneus (in terms of memory demands) set of jobs, they show different weaknesses: inefficient resource utilization, system performance degradation, and job resource competition.

`wava` scheduler is designed to overcome this, that is, without losing easy of use, providing guarantes for both system stability and job resource allocation and, on the other side, offering better resource utilization rates. 

## Features
### Capacity guarantees
Jobs are submitted with a minimum memory claim (job size), then enqueued, and finally executed in a sandboxed environment (implemented via [cgroups](https://en.wikipedia.org/wiki/Cgroups)) with a hard-limited capacity (scheduler capacity).

At runtime, the job process tree is allowed to allocate an amount of resident memory up to the scheduler-capacity (if no more jobs are scheduled), and in case of memory pressure forced to swap out.

The scheduler guareantees that each job process tree has at its disposition at least the amount of minimum memory claimed at job submission.

### Security
Job processes are run by the same machine user that submitted the job, so the scheduler can not be used to [escale the running privileges](https://en.wikipedia.org/wiki/Privilege_escalation) of a user.

### Resource-based scheduling
Scheduling is based on memory. The scheduler is configured to have a certain capacity and each job has an associated minimum memory size. 

The main **scheduling constraint** is the following: the sum of the running jobs minimum memory size cannot exceed the scheduler capacity. 

### Priority-based scheduling
This feature allows jobs to be submitted and scheduled with different priorities. 

All jobs belong (implicity or explicity) to a [priority group]((#priority-and-groups)) that determines their global ordering, used for positioning in the queue and assigning a process niceness when running.

## Architecture

The scheduler runs as a centralized process (`wava -s`) and all interaction with it is performed by separated client processes started when invoking the other command options.  

In particular job submissions are performed by separate peer processes (`wava -r`) that serve as lightweight placeholders of the real jobs executed by the scheduler. Peer and job processes have their lifecycle bound to each other. If one dies the other dies too.

![wava menu](https://github.com/brutusin/wava/raw/master/img/process-diagram.png)

The scheduler pipes standard io-streams between the job processes and their respective peer processes. Additionally, it pipes scheduler [events](#events) to the peer `stderr` unless an event file has been specified in submission (`wava -r -e <file>`).

![wava example](https://github.com/brutusin/wava/raw/master/img/wava-example2.gif)
*Running an example command requiring a minimum of 100MB. Observe also that this command runs untils user 'nacho' cancels it, returning a non-zero return code*

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
------------------   | --- | -----
`id`                 | yes | Id assigned to the job.
`queued`             | yes | Position in the queue, if the job is queued.
`priority`           | yes | Piority of the job, given by its group. 
`running`            | yes | Root pId of the job process when started.  
`niceness`           | yes | Niceness set to the job process.   
`cancelled`          | yes | User cancelling the job. 
`ping`               | no  | Send periodically to detect stale peers.
`exceed_tree        `| yes | Memory claim exceeds capacity
`shutdown`           | yes | Scheduler is being stopped 
`maxrss`             | yes | Max RSS allocated to the process tree
`maxswap`            | yes | Max swap allocated to the process tree
`error`              | yes | To send information about an error.
`retcode`            | yes | Return code for the client process to use.
`starvation_relaunch`| yes | Indicates that the job has been reenqueued due to a [starvation scenario](#deadlock-prevention) (applies for idempotent jobs)
`starvation_stop`    | yes | Indicates that the job has been stopped due to a starvation scenario (applies for non-idempotent jobs)

## Job hierarchy
Running jobs can submit more jobs, thus a job hierarchy is established. This potentially can lead to deadlock scenarios, when a parent (running) job waits for a child job (queded) to finish.

### Blocked state
 All parent jobs with no children running are considered blocked, ie waiting for their queued children to finish.

### Deadlock prevention
In order to avoid deadlock, and prevent from starvation (having too much jobs blocked by a waiting children), the follows a series of rules that may force a running blocking job to be requeued (if submitted as 'idempotent') or even stoped.

First the candidate job to be preempted is chosen based on its idempotency (idempotent first) and priority (low priority first) and in case that the maximum ratio between the sum of memory claims of all the blocked jobs divided by the scheduler capacity exceeds a configurable value, the scenario is considered as starving, and the candidate is preempted to make room for a potentially blocking job to run.

## Requirements
`$JAVA_HOME` environment variable set pointing to a JRE 8+

## Installation
### 0. Run as `root`
```sh
sudo su
```
### 1. Create the `WAVA_HOME` environment variable pointing to the desired installation folder:
```sh
export WAVA_HOME=/opt/wava
```
make this variable persistent adding the previous line to the file: `~root/.bashrc`


### 2. Download latest version:
```sh
export WAVA_VERSION=`wget -O - -o /dev/null https://repo1.maven.org/maven2/org/brutusin/wava-core/maven-metadata.xml | grep '<release>' | grep -Eow '[0-9\.]*'`
wget -O /tmp/wava-latest-dist.zip "https://repository.sonatype.org/service/local/artifact/maven/content?r=central-proxy&g=org.brutusin&a=wava-core&c=dist&e=zip&v=$WAVA_VERSION"
```
*This `$WAVA_VERSION` variable has been created only for installation purposes and doesn't need to be persisted for future sessions*
### 3. Decompress the distribution zip:
```sh
unzip -o /tmp/wava-latest-dist.zip -d /tmp
```
### 4. Set appropriate file permissions:
```sh
chmod -R 555 /tmp/wava-core-$WAVA_VERSION
```

### 5. Move and create symbolic links:
```sh
mkdir -p $WAVA_HOME
mv /tmp/wava-core-$WAVA_VERSION/* $WAVA_HOME
rm -rf /usr/bin/wava
cp $WAVA_HOME/bin/wava-home-aware /usr/bin/wava
```

### 6. Run to verify installation and generate default configuration file:
```sh
wava
```

### 7. Run on startup
Optionally, create a service to run the following command at startup by the "root" user: `wava -s`. Details are not given here since it varies depending on the Linux distribution.

## Configuration
Configuration is set in file: `$WAVA_HOME/cfg/wava.json`. Environment variables can be used in this file.
### Default configuration

```javascript
{
  "tempFolder" : "/dev/shm/wava/$WAVA_HOME",
  "uICfg" : {
    "ansiColors" : true,
    "sIMemoryUnits" : true
  },
  "schedulerCfg" : {
    "nicenessHandlerClassName" : "org.brutusin.wava.core.plug.impl.niceness.HomogeneusSpreadNicenessHandler",
    "memoryCgroupBasePath" : "/sys/fs/cgroup/memory/wava/$WAVA_HOME",
    "refreshLoopSleepMillisecs" : 10,
    "pingMillisecs" : 1000,
    "maxTotalRSSBytes" : -1,
    "maxJobRSSBytes" : -1,
    "commandTTLCacheSecs" : 2,
    "sigKillDelaySecs" : 5,
    "maxBlockedRssStarvationRatio" : 0.5
  },
  "processCfg" : {
    "nicenessRange" : [ 1, 19 ],
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
Property                                    | Description
--------                                    | -----------
`uICfg.ansiColors`                          | Use ANSI escape code sequences to highlight UI.
`uICfg.sIMemoryUnits`                       | Use units from the International System for output memory values. `true`: kB based, `false`:[KiB](https://en.wikipedia.org/wiki/Kibibyte) based
`schedulerCfg.nicenessHandlerClassName`     | FQN of the [`NicenessHandler`](src/main/java/org/brutusin/wava/core/plug/NicenessHandler.java) implementation (see [`impl`](https://github.com/brutusin/wava/tree/master/src/main/java/org/brutusin/wava/core/plug/impl) package) to use.
`schedulerCfg.memoryCgroupBasePath`         | Root path to the parent memory cgroup
`schedulerCfg.refreshLoopSleepMillisecs`    | Sleeping time for the main looping thread.
`schedulerCfg.pingMillisecs`                | Time interval between ping events to peer processes.
`schedulerCfg.maxTotalRSSBytes`             | Scheduler capacity. Maximum amount of physical memory permitted for all jobs. If `-1` the total amount of physical memory is considered.
`schedulerCfg.maxJobRSSBytes`               | Maximum value for a job memory claim. If `-1` there is no limit.
`schedulerCfg.commandTTLCacheSecs`          | Cache TTL in seconds, for some commands used to query information to the system.
`schedulerCfg.sigKillDelaySecs`             | Seconds between SIGTERM and SIGKILL signals send in job cancellation.
`schedulerCfg.maxBlockedRssStarvationRatio` | Maximum ratio between the sum of memory claims of the blocked jobs divided by the scheduler capacity. If exceeded the [starvation prevention mechanism](#deadlock-prevention) is triggered.
`processCfg.nicenessRange`                  | Minimum (most favorable) and maximum (less favorable) niceness to be assigned to a job process tree
`processCfg.cpuAfinity`                     | CPU affinity to be set to the job processes. In a format supported by the `-c` parameter of [taskset](http://linuxcommand.org/man_pages/taskset1.html).
`groupCfg.dynamicGroupIdleSeconds`          | Idle time for [dynamic groups](#priority-and-groups) in seconds.
`groupCfg.predefinedGroups`                 | Set of groups to be available since startup.
## Support bugs and requests
https://github.com/brutusin/wava/issues

## Authors

- Ignacio del Valle Alles (<https://github.com/idelvall/>)

Contributions are always welcome and greatly appreciated!

##License
Apache License, Version 2.0
