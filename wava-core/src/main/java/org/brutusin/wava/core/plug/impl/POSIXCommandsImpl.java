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
package org.brutusin.wava.core.plug.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.commons.utils.ProcessException;
import org.brutusin.commons.utils.ProcessUtils;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.core.plug.LinuxCommands;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class POSIXCommandsImpl extends LinuxCommands {

    private static String executeBashCommand(String command) throws IOException, InterruptedException {
        String[] cmd = {"/bin/bash", "-c", command};
        Process p = Runtime.getRuntime().exec(cmd);
        String[] ret = ProcessUtils.execute(p);
        return ret[0];
    }

    @Override
    public void setNiceness(int pId, int niceness) throws IOException, InterruptedException {
        try {
            String[] cmd = {"renice", "-n", String.valueOf(niceness), "-p", String.valueOf(pId)};
            Runtime.getRuntime().exec(cmd);
            Process p = Runtime.getRuntime().exec(cmd);
            ProcessUtils.execute(p);
            Process getChildrenProcess = Runtime.getRuntime().exec(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", String.valueOf(pId)});
            String output = ProcessUtils.execute(getChildrenProcess)[0];
            if (output != null) {
                String[] childrenIds = output.split("\n");
                for (String childrenId : childrenIds) {
                    setNiceness(Integer.valueOf(childrenId.trim()), niceness);
                }
            }
        } catch (ProcessException pe) {
            // Ignore process non-zero retcodes
        }
    }

    @Override
    public void setImmutable(File f, boolean immutable) throws IOException, InterruptedException {
        String[] cmd = {"chattr", immutable ? "+i" : "-i", f.getAbsolutePath()};
        Process p = Runtime.getRuntime().exec(cmd);
        ProcessUtils.execute(p);
    }

    @Override
    public void killTree(int pId) throws IOException, InterruptedException {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    Set<Integer> visitedIds = new HashSet<>();
                    getTree(visitedIds, pId, false);
                    kill(visitedIds, 15); // SIGTERM
                } catch (InterruptedException ex) {
                }
            }
        };
        t1.setName("SIGTERM " + pId);
        t1.start();
        Thread t2 = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000 * Config.getInstance().getSchedulerCfg().getSigKillDelaySecs());
                    Set<Integer> visitedIds = new HashSet<>();
                    getTree(visitedIds, pId, true);
                    kill(visitedIds, 9); // SIGKILL
                } catch (InterruptedException ex) {
                }
            }
        };
        t2.setName("SIGKILL " + pId);
        t2.start();

    }

    public void getTree(Set<Integer> visited, int pId, boolean stop) throws InterruptedException {
        try {
            if (stop) {
                // needed to stop quickly forking parent from producing children between child killing and parent killing
                Process stopProcess = Runtime.getRuntime().exec(new String[]{"kill", "-stop", String.valueOf(pId)});
                ProcessUtils.execute(stopProcess);
            }
            Process getChildrenProcess = Runtime.getRuntime().exec(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", String.valueOf(pId)});
            String output = ProcessUtils.execute(getChildrenProcess)[0];
            if (output != null) {
                String[] pIds = output.split("\n");
                for (int i = 0; i < pIds.length; i++) {
                    getTree(visited, Integer.valueOf(pIds[i].trim()), stop);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                throw (InterruptedException) ex;
            } else {
                // Silently continue if executed commands don't return 0
            }
        } finally {
            visited.add(pId);
        }
    }

    private void kill(Set<Integer> pIds, int signal) throws InterruptedException {
        try {
            String[] cmd = new String[3 + pIds.size()];
            cmd[0] = "kill";
            cmd[1] = "-s";
            cmd[2] = String.valueOf(signal);
            Iterator<Integer> it = pIds.iterator();
            for (int i = 3; i < cmd.length; i++) {
                cmd[i] = String.valueOf(it.next());
            }
            Process p = Runtime.getRuntime().exec(cmd);
            ProcessUtils.execute(p);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                throw (InterruptedException) ex;
            } else {
                // Silently continue if executed command doesn't return 0
            }
        }
    }

    @Override
    public long getSystemRSSUsedMemory() throws IOException, InterruptedException {
        return getSystemRSSMemory() - getSystemRSSFreeMemory();
    }

    @Override
    public long[] getTreeRSS(int[] pIds) throws IOException, InterruptedException {
        long[] ret = new long[pIds.length];
        Map<Integer, Integer> indexes = new HashMap<>();
        for (int i = 0; i < pIds.length; i++) {
            indexes.put(pIds[i], i);
        }
        String[] cmd = {"ps", "axo", "pid,ppid,rss", "--no-headers", "--sort=start_time"};
        Process p = Runtime.getRuntime().exec(cmd);
        String stdout;
        try {
            stdout = ProcessUtils.execute(p)[0];
        } catch (RuntimeException ex) {
            // no processes exist retcode=1
            return null;
        }
        if (stdout != null) {
            String[] lines = stdout.split("\n");
            for (String line : lines) {
                String[] cols = line.trim().split("\\s+");
                Integer pid = Integer.valueOf(cols[0].trim());
                Integer index = indexes.get(pid);
                if (index == null) {
                    Integer ppid = Integer.valueOf(cols[1].trim());
                    index = indexes.get(ppid);
                    if (index != null) {
                        indexes.put(pid, index);
                    }
                }
                if (index != null) {
                    ret[index] += Long.valueOf(cols[2].trim()) * 1000;
                }
            }
        }
        return ret;
    }

    @Override
    public String[] decorateWithCPUAffinity(String[] cmd, String affinity) {
        String[] ret = new String[cmd.length + 3];
        ret[0] = "taskset";
        ret[1] = "-c";
        ret[2] = affinity;
        for (int i = 3; i < ret.length; i++) {
            ret[i] = cmd[i - 3];
        }
        return ret;
    }

    @Override
    public long getSystemRSSFreeMemory() throws IOException, InterruptedException {
        String ouput = executeBashCommand("echo $((`cat /proc/meminfo | grep ^Cached:| awk '{print $2}'` + `cat /proc/meminfo | grep ^MemFree:| awk '{print $2}'` ))");
        return Long.valueOf(ouput.trim()) * 1000;
    }

    @Override
    public long getSystemRSSMemory() throws IOException, InterruptedException {
        String ouput = executeBashCommand("cat /proc/meminfo | grep ^MemTotal:| awk '{print $2}'");
        return Long.valueOf(ouput.trim()) * 1000;
    }

    @Override
    public String[] getRunAsCommand(String user, String[] cmd) {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < cmd.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append("\"").append(cmd[i]).append("\"");
        }
        return new String[]{"runuser", "-p", user, "-c", sb.toString()};
    }

    @Override
    public String getRunningUser() throws IOException, InterruptedException {
        String[] cmd = {"id", "-un"};
        Process p = Runtime.getRuntime().exec(cmd);
        return ProcessUtils.execute(p)[0];
    }

    @Override
    public String getFileOwner(File f) throws IOException, InterruptedException {
        return executeBashCommand("ls -ld \"" + f.getAbsolutePath() + "\" | awk 'NR==1 {print $3}'");
    }
}
