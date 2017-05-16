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

import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.cfg.SchedulerCfg;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class SchedulerCfgImpl implements SchedulerCfg {

    private String nicenessHandlerClassName;
    private String cgroupRootPath;
    private int refreshLoopSleepMillisecs = 10;
    private int pingMillisecs = 1000;
    private String schedulerCapacity = "$DEFAULT_CAPACITY";
    private String maxSwap = "$DEFAULT_SWAP";
    private String maxJobSize = "$DEFAULT_CAPACITY";
    private boolean outOfMemoryKillerEnabled = false;
    private float maxBlockedRssStarvationRatio = 0.5f;
    private String logFolder = "/tmp/wava";
    private String loggingLevel = "FINE";
    private String maxLogSize = "100MB";
    private transient long _maxLogSize = -1;
    private String maxStatsLogSize = "100MB";
    private transient long _maxStatsLogSize = -1;
    private int statsCpuStep = 15;
    private String statsRssStep = "50MB";
    private transient long _statsRssStep = -1;
    private String statsSwapStep = "50MB";
    private transient long _statsSwapStep = -1;
    private String statsIOStep = "50MB";
    private transient long _statsIOStep = -1;

    private boolean logStats = false;

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
    public String getCgroupRootPath() {
        return cgroupRootPath;
    }

    public void setCgroupRootPath(String cgroupRootPath) {
        this.cgroupRootPath = cgroupRootPath;
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

    @Override
    public String getLogFolder() {
        return logFolder;
    }

    public void setLogFolder(String logFolder) {
        this.logFolder = logFolder;
    }

    @Override
    public String getLoggingLevel() {
        return loggingLevel;
    }

    public void setLoggingLevel(String loggingLevel) {
        this.loggingLevel = loggingLevel;
    }

    @Override
    public long getMaxLogSize() {
        if (_maxLogSize < 0) {
            _maxLogSize = Miscellaneous.parseHumanReadableByteCount(maxLogSize);
        }
        return _maxLogSize;
    }

    public void setMaxLogSize(String maxLogSize) {
        this.maxLogSize = maxLogSize;
    }

    @Override
    public long getMaxStatsLogSize() {
        if (_maxStatsLogSize < 0) {
            _maxStatsLogSize = Miscellaneous.parseHumanReadableByteCount(maxStatsLogSize);
        }
        return _maxStatsLogSize;
    }

    public void setMaxStatsLogSize(String maxStatsLogSize) {
        this.maxStatsLogSize = maxStatsLogSize;
    }

    @Override
    public boolean isLogStats() {
        return logStats;
    }

    public void setLogStats(boolean logStats) {
        this.logStats = logStats;
    }

    @Override
    public int getStatsCpuStep() {
        return statsCpuStep;
    }

    public void setStatsCpuStep(int statsCpuStep) {
        this.statsCpuStep = statsCpuStep;
    }

    @Override
    public long getStatsRssStep() {
        if (_statsRssStep < 0) {
            _statsRssStep = Miscellaneous.parseHumanReadableByteCount(statsRssStep);
        }
        return _statsRssStep;
    }

    public void setStatsRssStep(String statsRssStep) {
        this.statsRssStep = statsRssStep;
    }

    @Override
    public long getStatsSwapStep() {
        if (_statsSwapStep < 0) {
            _statsSwapStep = Miscellaneous.parseHumanReadableByteCount(statsSwapStep);
        }
        return _statsSwapStep;
    }

    public void setStatsSwapStep(String statsSwapStep) {
        this.statsSwapStep = statsSwapStep;
    }

    @Override
    public long getStatsIOStep() {
        if (_statsIOStep < 0) {
            _statsIOStep = Miscellaneous.parseHumanReadableByteCount(statsIOStep);
        }
        return _statsIOStep;
    }

    public void setStatsIOStep(String statsIOStep) {
        this.statsIOStep = statsIOStep;
    }
}
