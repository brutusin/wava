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
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
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
public class POSIXCommandsImpl extends LinuxCommands {

    private static final File FILE_MEMINFO = new File("/proc/meminfo");

    private static String executeBashCommand(String command) throws ProcessException, InterruptedException {
        String[] cmd = {"/bin/bash", "-c", command};
        return ProcessUtils.executeProcess(cmd);
    }

    @Override
    public void setNiceness(int pId, int niceness) {
        try {
            String[] cmd = {"renice", "-n", String.valueOf(niceness), "-p", String.valueOf(pId)};
            ProcessUtils.executeProcess(cmd);
            String output = ProcessUtils.executeProcess(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", String.valueOf(pId)});
            if (output != null) {
                String[] childrenIds = output.split("\n");
                for (String childrenId : childrenIds) {
                    setNiceness(Integer.valueOf(childrenId.trim()), niceness);
                }
            }
        } catch (ProcessException ex) {
            // Silently continue if executed command doesn't return 0
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void killTree(int pId) {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                Set<Integer> visitedIds = new HashSet<>();
                getTree(visitedIds, pId, false);
                kill(visitedIds, 15); // SIGTERM
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

    private void getTree(Set<Integer> visited, int pId, boolean stop) {
        try {
            if (stop) {
                // needed to stop quickly forking parent from producing children between child killing and parent killing
                ProcessUtils.executeProcess(new String[]{"kill", "-stop", String.valueOf(pId)});
            }
            String output = ProcessUtils.executeProcess(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", String.valueOf(pId)});
            if (output != null) {
                String[] pIds = output.split("\n");
                for (int i = 0; i < pIds.length; i++) {
                    getTree(visited, Integer.valueOf(pIds[i].trim()), stop);
                }
            }
        } catch (ProcessException ex) {
            // Silently continue if executed command doesn't return 0
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            visited.add(pId);
        }
    }

    private void kill(Set<Integer> pIds, int signal) {
        try {
            String[] cmd = new String[3 + pIds.size()];
            cmd[0] = "kill";
            cmd[1] = "-s";
            cmd[2] = String.valueOf(signal);
            Iterator<Integer> it = pIds.iterator();
            for (int i = 3; i < cmd.length; i++) {
                cmd[i] = String.valueOf(it.next());
            }
            ProcessUtils.executeProcess(cmd);
        } catch (ProcessException ex) {
            // Silently continue if executed command doesn't return 0
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public long[] getTreeRSS(int[] pIds) {
        long[] ret = new long[pIds.length];
        Map<Integer, Integer> indexes = new HashMap<>();
        for (int i = 0; i < pIds.length; i++) {
            indexes.put(pIds[i], i);
        }
        String[] cmd = {"ps", "axo", "pid,ppid,rss", "--no-headers", "--sort=start_time"};
        String stdout;
        try {
            stdout = ProcessUtils.executeProcess(cmd);
        } catch (ProcessException ex) {
            // no processes exist retcode=1
            return null;
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
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
    public String[] decorateWithBatchSchedulerPolicy(String[] cmd) {
        String[] ret = new String[cmd.length + 3];
        ret[0] = "chrt";
        ret[1] = "-b";
        ret[2] = "0";
        for (int i = 3; i < ret.length; i++) {
            ret[i] = cmd[i - 3];
        }
        return ret;
    }

    @Override
    public long[] getMemInfo() {
        long[] ret = {-1, -1};
        try (Scanner scanner = new Scanner(FILE_MEMINFO).useDelimiter("\\s*:\\s*|\n")) {
            while (scanner.hasNext()) {
                String token = scanner.next();
                if (token.equals("MemTotal")) {
                    ret[0] = Miscellaneous.parseHumanReadableByteCount(scanner.next());
                } else if (token.equals("MemAvailable")) {
                    ret[1] = Miscellaneous.parseHumanReadableByteCount(scanner.next());
                }
                if (ret[0] != -1 && ret[1] != -1) {
                    break;
                }
            }
            return ret;
        } catch (FileNotFoundException e) {
            throw new Error(FILE_MEMINFO.getPath() + " not found");
        }
    }

    @Override
    public String[] decorateRunAsCommand(String[] cmd, String user) {
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
    public String getRunningUser() {
        try {
            String[] cmd = {"id", "-un"};
            return ProcessUtils.executeProcess(cmd);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String getFileOwner(File f) {
        try {
            return executeBashCommand("ls -ld \"" + f.getAbsolutePath() + "\" | awk 'NR==1 {print $3}'");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
