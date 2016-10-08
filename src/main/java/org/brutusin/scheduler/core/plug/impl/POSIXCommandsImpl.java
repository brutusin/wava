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
package org.brutusin.scheduler.core.plug.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.commons.utils.ProcessUtils;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import org.brutusin.scheduler.data.Stats;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class POSIXCommandsImpl extends LinuxCommands {

    private static final int SIGKILL_DELAY_SECONDS = 5;

    private static String getPIdList(int[] pIds) {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < pIds.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(pIds[i]);
        }
        return sb.toString();
    }

    private static String getPIdList(Set<Integer> pIds) {
        StringBuilder sb = new StringBuilder("");
        for (Integer pId : pIds) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(pId);
        }
        return sb.toString();
    }

    private static String executeBashCommand(String command) throws IOException, InterruptedException {
        String[] cmd = {"/bin/bash", "-c", command};
        Process p = Runtime.getRuntime().exec(cmd);
        String[] ret = ProcessUtils.execute(p);
        return ret[0];
    }

    /**
     *
     * killtree() { local _pid=$1 local _sig=${2:--TERM} kill -stop ${_pid}
     * 2>/dev/null # needed to stop quickly forking parent from producing
     * children between child killing and parent killing for _child in $(ps -o
     * pid --no-headers --ppid ${_pid}); do killtree ${_child} ${_sig}
     * 2>/dev/null done kill -${_sig} ${_pid} 2>/dev/null }
     *
     * if [ $# -eq 0 -o $# -gt 2 ]; then echo "Usage: $(basename $0) <pid>
     * [signal]" exit 1 fi
     *
     * @param pId
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void killTree(int pId) throws IOException, InterruptedException {
        final Set<Integer> visitedIds = new HashSet<>();
        getAndStopTree(visitedIds, pId);
        kill(visitedIds, 15); // SIGTERM
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000 * SIGKILL_DELAY_SECONDS);
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
            Process p = Runtime.getRuntime().exec(new String[]{"kill", "-s", String.valueOf(signal), getPIdList(pIds)});
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
    public Map<String, Stats> getStats(int[] pids) throws IOException, InterruptedException {
        Map<String, Stats> ret = new HashMap<String, Stats>();
        String[] cmd = {"ps", "-p", getPIdList(pids), "-o", "pid,rss,pcpu", "--no-headers"};
        Process p = Runtime.getRuntime().exec(cmd);
        String stdout = ProcessUtils.execute(p)[0];
        if (stdout != null) {
            String[] lines = stdout.split("\n");
            for (int i = 0; i < lines.length; i++) {
                String[] cols = lines[i].split("\\s+");
                Stats st = new Stats();
                st.setRssBytes(Integer.valueOf(cols[1]));
                st.setCpuPercent(Double.valueOf(cols[2]));
                ret.put(cols[0], st);
            }
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
