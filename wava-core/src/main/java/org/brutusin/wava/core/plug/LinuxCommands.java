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
package org.brutusin.wava.core.plug;

import java.io.File;
import org.brutusin.wava.core.plug.impl.POSIXCommandsImpl;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public abstract class LinuxCommands {

    private static final LinuxCommands INSTANCE = new POSIXCommandsImpl();

    public static LinuxCommands getInstance() {
        return INSTANCE;
    }

    public abstract boolean createWavaMemoryCgroup();

    public abstract void removeJobMemoryCgroup(int jobId);

    public abstract void createJobMemoryCgroup(int jobId, long maxRSSBytes);

    public abstract void setNiceness(int pId, int niceness);

    public abstract String[] decorateRunAsCommand(String[] cmd, String user);

    public abstract String[] decorateWithCPUAffinity(String[] cmd, String affinity);

    public abstract String[] decorateWithBatchSchedulerPolicy(String[] cmd);

    public abstract String[] decorateRunInCgroup(String[] cmd, int jobId);

    public abstract TreeStats[] getTreeStats(int[] pids);

    public abstract CgroupMemoryStats getCgroupMemoryStats(int jobId);

    public abstract void killTree(int pid);

    /**
     * Returns an array of length 2 being ret[0]: total RAM, and ret[1]:
     * available RAM
     *
     * @return
     */
    public abstract long[] getMemInfo();

    public abstract String getFileOwner(File f);

    public abstract String getRunningUser();

    public static class TreeStats {

        public long rssBytes;
        public double cpuPercentage;
    }

    public static class CgroupMemoryStats {

        public long rssBytes;
        public long swapBytes;
    }
}
