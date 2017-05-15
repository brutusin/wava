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
package org.brutusin.wava.cfg;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public interface SchedulerCfg {

    public String getCgroupRootPath();

    public String getLogFolder();

    public String getLoggingLevel();

    public long getMaxLogSize();

    public boolean isLogStats();

    public int getStatsCpuStep();

    public long getStatsRssStep();

    public long getStatsSwapStep();

    public long getStatsIOStep();

    public long getMaxStatsLogSize();

    public String getNicenessHandlerClassName();

    public int getRefreshLoopSleepMillisecs();

    public int getPingMillisecs();

    public float getMaxBlockedRssStarvationRatio();

    public String getSchedulerCapacity();

    public String getMaxSwap();

    public String getMaxJobSize();

    public boolean isOutOfMemoryKillerEnabled();

    public int getUserHz();
}
