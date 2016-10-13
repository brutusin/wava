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
package org.brutusin.wava.core.cfg;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class ConfigImpl {

    private boolean ansiColors = true;
    private int pollingSecs = 5;
    private int maxTotalRSSBytes = -1;
    private int commandTTLCacheSecs = 2;
    private int sigKillDelaySecs = 5;
    private int dynamicGroupIdleSeconds = 10;
    private int[] nicenessRange = {-20,19};
    private String cpuAfinity = "0-" + (Runtime.getRuntime().availableProcessors() - 1);

    public boolean isAnsiColors() {
        return ansiColors;
    }

    public void setAnsiColors(boolean ansiColors) {
        this.ansiColors = ansiColors;
    }

    public int getPollingSecs() {
        return pollingSecs;
    }

    public void setPollingSecs(int pollingSecs) {
        this.pollingSecs = pollingSecs;
    }

    public int getMaxTotalRSSBytes() {
        return maxTotalRSSBytes;
    }

    public void setMaxTotalRSSBytes(int maxTotalRSSBytes) {
        this.maxTotalRSSBytes = maxTotalRSSBytes;
    }

    public int getCommandTTLCacheSecs() {
        return commandTTLCacheSecs;
    }

    public void setCommandTTLCacheSecs(int commandTTLCacheSecs) {
        this.commandTTLCacheSecs = commandTTLCacheSecs;
    }

    public int getSigKillDelaySecs() {
        return sigKillDelaySecs;
    }

    public void setSigKillDelaySecs(int sigKillDelaySecs) {
        this.sigKillDelaySecs = sigKillDelaySecs;
    }

    public String getCpuAfinity() {
        return cpuAfinity;
    }

    public void setCpuAfinity(String cpuAfinity) {
        this.cpuAfinity = cpuAfinity;
    }

    public int getDynamicGroupIdleSeconds() {
        return dynamicGroupIdleSeconds;
    }

    public void setDynamicGroupIdleSeconds(int dynamicGroupIdleSeconds) {
        this.dynamicGroupIdleSeconds = dynamicGroupIdleSeconds;
    }

    public int[] getNicenessRange() {
        return nicenessRange;
    }

    public void setNicenessRange(int[] nicenessRange) {
        this.nicenessRange = nicenessRange;
    }
}
