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
import org.brutusin.commons.utils.ProcessUtils;
import org.brutusin.wava.core.cfg.Config;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.data.Stats;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class POSIXCommandsImpl extends LinuxCommands {

    private static String executeBashCommand(String command) throws IOException, InterruptedException {
        String[] cmd = {"/bin/bash", "-c", command};
        Process p = Runtime.getRuntime().exec(cmd);
        String[] ret = ProcessUtils.execute(p);
        return ret[0];
    }

    @Override
    public void setNiceness(int pId, int niceness) throws IOException, InterruptedException {
        String[] cmd = {"renice", "-n", String.valueOf(niceness), "-p", String.valueOf(pId)};
        Process p = Runtime.getRuntime().exec(cmd);
        ProcessUtils.execute(p);
    }

    @Override
    public void setImmutable(File f, boolean immutable) throws IOException, InterruptedException {
        String[] cmd = {"chattr", immutable ? "+i" : "-i", f.getAbsolutePath()};
        Process p = Runtime.getRuntime().exec(cmd);
        ProcessUtils.execute(p);
    }

    @Override
    public void killTree(int pId) throws IOException, InterruptedException {
        final Set<Integer> visitedIds = new HashSet<>();
        getAndStopTree(visitedIds, pId);
        kill(visitedIds, 15); // SIGTERM
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000 * Config.getInstance().getSigKillDelaySecs());
                    kill(visitedIds, 9); // SIGKILL
                } catch (InterruptedException ex) {
                }
            }
        };
        t.setName("killtree " + pId);
        t.start();

    }

    public void getAndStopTree(Set<Integer> visited, int pId) throws InterruptedException {
        try {
            // needed to stop quickly forking parent from producing children between child killing and parent killing
            Process stopProcess = Runtime.getRuntime().exec(new String[]{"kill", "-stop", String.valueOf(pId)});
            ProcessUtils.execute(stopProcess);
            Process getChildrenProcess = Runtime.getRuntime().exec(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", String.valueOf(pId)});
            String output = ProcessUtils.execute(getChildrenProcess)[0];
            if (output != null) {
                String[] pIds = output.split("\n");
                for (int i = 0; i < pIds.length; i++) {
                    getAndStopTree(visited, Integer.valueOf(pIds[i]));
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
    public Map<Integer, Stats> getStats(int[] pIds) throws IOException, InterruptedException {
        Map<Integer, Stats> ret = new HashMap<>();
        String[] cmd = new String[pIds.length + 5];
        cmd[0] = "ps";
        cmd[1] = "-o";
        cmd[2] = "pid,rss,pcpu";
        cmd[3] = "--no-headers";
        cmd[4] = "-p";
        for (int i = 5; i < cmd.length; i++) {
            cmd[i] = String.valueOf(pIds[i - 5]);
        }
        Process p = Runtime.getRuntime().exec(cmd);
        String stdout = null;
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
                Stats st = new Stats();
                st.setRssBytes(Integer.valueOf(cols[1]));
                st.setCpuPercent(Double.valueOf(cols[2]));
                ret.put(Integer.valueOf(cols[0]), st);
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
        return Long.valueOf(ouput);
    }

    @Override
    public long getSystemRSSMemory() throws IOException, InterruptedException {
        String ouput = executeBashCommand("cat /proc/meminfo | grep ^MemTotal:| awk '{print $2}'");
        return Long.valueOf(ouput);
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
    public String[] getRunWithNicenessCommand(int niceness, String[] cmd) {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < cmd.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append("\"").append(cmd[i]).append("\"");
        }
        return new String[]{"nice", "-n", String.valueOf(niceness), sb.toString()};
    }
    
    

    @Override
    public String getRunningUser() throws IOException, InterruptedException {
        String[] cmd = {"id", "-un"};
        Process p = Runtime.getRuntime().exec(cmd);
        return ProcessUtils.execute(p)[0];
    }

    @Override
    public void createNamedPipes(File... files) throws IOException, InterruptedException {
        String[] mkfifo = new String[files.length + 1];
        String[] chmod = new String[files.length + 2];
        mkfifo[0] = "mkfifo";
        chmod[0] = "chmod";
        chmod[1] = "777";
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (!f.getParentFile().exists()) {
                Miscellaneous.createDirectory(f.getParentFile());
            }
            mkfifo[i + 1] = f.getAbsolutePath();
            chmod[i + 2] = f.getAbsolutePath();
        }
        Process p = Runtime.getRuntime().exec(mkfifo);
        ProcessUtils.execute(p);
        p = Runtime.getRuntime().exec(chmod);
        ProcessUtils.execute(p);
    }

    @Override
    public String getFileOwner(File f) throws IOException, InterruptedException {
        return executeBashCommand("ls -ld \"" + f.getAbsolutePath() + "\" | awk 'NR==1 {print $3}'");
    }

}
