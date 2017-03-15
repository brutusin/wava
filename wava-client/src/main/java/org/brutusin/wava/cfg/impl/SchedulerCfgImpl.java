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

    private String nicenessHandlerClassName;
    private String memoryCgroupBasePath;
    private int refreshLoopSleepMillisecs = 10;
    private int pingMillisecs = 1000;
    private String schedulerCapacity = "$DEFAULT_CAPACITY";
    private String maxSwap = "$DEFAULT_SWAP";
    private String maxJobSize = "$DEFAULT_CAPACITY";
    private boolean outOfMemoryKillerEnabled = false;
    private int sigKillDelaySecs = 5;
    private float maxBlockedRssStarvationRatio = 0.5f;

    @Override
    public String getSchedulerCapacity() {
        return schedulerCapacity;
    }

    public void setSchedulerCapacity(String schedulerCapacity) {
        this.schedulerCapacity = schedulerCapacity;
    }

    @Override
    public String getMaxSwap() {
        return maxSwap;
    }

    public void setMaxSwap(String maxSwap) {
        this.maxSwap = maxSwap;
    }

    @Override
    public String getMaxJobSize() {
        return maxJobSize;
    }

    public void setMaxJobSize(String maxJobSize) {
        this.maxJobSize = maxJobSize;
    }

    @Override
    public String getMemoryCgroupBasePath() {
        return memoryCgroupBasePath;
    }

    public void setMemoryCgroupBasePath(String memoryCgroupBasePath) {
        this.memoryCgroupBasePath = memoryCgroupBasePath;
    }

    @Override
    public String getNicenessHandlerClassName() {
        return nicenessHandlerClassName;
    }

    public void setNicenessHandlerClassName(String nicenessHandlerClassName) {
        this.nicenessHandlerClassName = nicenessHandlerClassName;
    }

    @Override
    public boolean isOutOfMemoryKillerEnabled() {
        return outOfMemoryKillerEnabled;
    }

    public void setOutOfMemoryKillerEnabled(boolean OutOfMemoryKillerEnabled) {
        this.outOfMemoryKillerEnabled = OutOfMemoryKillerEnabled;
    }

    @Override
    public int getRefreshLoopSleepMillisecs() {
        return refreshLoopSleepMillisecs;
    }

    public void setPollingMillisecs(int refreshLoopSleepMillisecs) {
        this.refreshLoopSleepMillisecs = refreshLoopSleepMillisecs;
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

    @Override
    public int getPingMillisecs() {
        return pingMillisecs;
    }

    public void setPingMillisecs(int pingMillisecs) {
        this.pingMillisecs = pingMillisecs;
    }
}
