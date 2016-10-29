#org.brutusin:wava [![Build Status](https://api.travis-ci.org/brutusin/wava.svg?branch=master)](https://travis-ci.org/brutusin/wava) [![Maven Central Latest Version](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.brutusin/wava/)

**[W]hen [AVA]ilable linux scheduler**: A RSS-memory-aware scheduler for Linux batch processes. 

`wava` allows to enqueue batch jobs (submitted with a maximum physical memory usage promise) to be executed when enough physical memory ([RSS](https://en.wikipedia.org/wiki/Resident_set_size)) is available in the system.

<p align="center">
    <img src="https://github.com/brutusin/wava/raw/master/img/wava.gif" alt="wava shiny logo">
</p>

This scheduler has been created originally to enqueue a high number of jobs in machines with a large amount of RAM, running concurrently the largest number of them but avoiding memory paging and swapping in order to not penalize the performance of other services running in the system.

(... to be continued)

## Requirements
JRE 8+

## Installation
1. Download latest version:
```sh
curl -O -J -L "https://repository.sonatype.org/service/local/artifact/maven/content?r=central-proxy&g=org.brutusin&a=wava&c=dist&e=zip&v=LATEST"
```
2. Decompress the distribution zip
```
unzip wava-*-dist.zip -d .
```



## Configuration

## Priority groups

## Support bugs and requests
https://github.com/brutusin/linux-scheduler/issues

## Authors

- Ignacio del Valle Alles (<https://github.com/idelvall/>)

Contributions are always welcome and greatly appreciated!

##License
Apache License, Version 2.0
