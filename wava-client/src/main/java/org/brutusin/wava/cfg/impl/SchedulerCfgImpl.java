/*
 * Copyright 2016 Ignacio del Valle Alles idelvall@brutusin.org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.brutusin.wava.cfg.impl;

import org.brutusin.wava.cfg.SchedulerCfg;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class SchedulerCfgImpl implements SchedulerCfg {

    private String promiseHandlerClassName;
    private String nicenessHandlerClassName;

    private int pollingMillisecs = 10;
    private long maxTotalRSSBytes = -1;
    private long maxJobRSSBytes = -1;
    private int commandTTLCacheSecs = 2;
    private int sigKillDelaySecs = 5;
    private float maxBlockedRssStarvationRatio = 0.5f;

    @Override
    public String getPromiseHandlerClassName() {
        return promiseHandlerClassName;
    }

    public void setPromiseHandlerClassName(String promiseHandlerClassName) {
        this.promiseHandlerClassName = promiseHandlerClassName;
    }

    @Override
    public String getNicenessHandlerClassName() {
        return nicenessHandlerClassName;
    }

    public void setNicenessHandlerClassName(String nicenessHandlerClassName) {
        this.nicenessHandlerClassName = nicenessHandlerClassName;
    }

    @Override
    public int getPollingMillisecs() {
        return pollingMillisecs;
    }

    public void setPollingMillisecs(int pollingMillisecs) {
        this.pollingMillisecs = pollingMillisecs;
    }

    @Override
    public long getMaxTotalRSSBytes() {
        return maxTotalRSSBytes;
    }

    public void setMaxTotalRSSBytes(long maxTotalRSSBytes) {
        this.maxTotalRSSBytes = maxTotalRSSBytes;
    }

    @Override
    public long getMaxJobRSSBytes() {
        return maxJobRSSBytes;
    }

    public void setMaxJobRSSBytes(long maxJobRSSBytes) {
        this.maxJobRSSBytes = maxJobRSSBytes;
    }

    @Override
    public int getCommandTTLCacheSecs() {
        return commandTTLCacheSecs;
    }

    public void setCommandTTLCacheSecs(int commandTTLCacheSecs) {
        this.commandTTLCacheSecs = commandTTLCacheSecs;
    }

    @Override
    public int getSigKillDelaySecs() {
        return sigKillDelaySecs;
    }

    public void setSigKillDelaySecs(int sigKillDelaySecs) {
        this.sigKillDelaySecs = sigKillDelaySecs;
    }

    @Override
    public float getMaxBlockedRssStarvationRatio() {
        return maxBlockedRssStarvationRatio;
    }

    public void setMaxBlockedRssStarvationRatio(float maxBlockedRssStarvationRatio) {
        this.maxBlockedRssStarvationRatio = maxBlockedRssStarvationRatio;
    }
}
