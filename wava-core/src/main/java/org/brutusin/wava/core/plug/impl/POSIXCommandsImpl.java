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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
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
import org.brutusin.wava.env.WavaHome;

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
    public boolean createWavaMemoryCgroup(long totalManagedRss) {
        try {
            File f = new File(Config.getInstance().getSchedulerCfg().getMemoryCgroupBasePath() + "/" + WavaHome.getInstance().getId());
            removeLeafFolder(f);
            String[] cmd = {"mkdir", f.getAbsolutePath()};
            ProcessUtils.executeProcess(cmd);
            Miscellaneous.writeStringToFile(new File(f, "memory.limit_in_bytes"), String.valueOf(totalManagedRss), "UTF-8");
            Miscellaneous.writeStringToFile(new File(f, "memory.use_hierarchy"), "1", "UTF-8");

            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private void removeLeafFolder(File folder) throws ProcessException, InterruptedException {
        if (!folder.exists()) {
            return;
        }
        File[] children = folder.listFiles();
        if (children != null) {
            for (int i = 0; i < children.length; i++) {
                File ch = children[i];
                if (ch.isDirectory()) {
                    removeLeafFolder(ch);
                }
            }
        }
        String[] cmd = {"rmdir", folder.getAbsolutePath()};
        ProcessUtils.executeProcess(cmd);
    }

    @Override
    public void createJobMemoryCgroup(int jobId, long maxJobRSSBytes) {
        try {
            File f = new File(Config.getInstance().getSchedulerCfg().getMemoryCgroupBasePath() + "/" + WavaHome.getInstance().getId() + "/" + String.valueOf(jobId));
            String[] cmd = {"mkdir", f.getAbsolutePath()};
            ProcessUtils.executeProcess(cmd);
            Miscellaneous.writeStringToFile(new File(f, "memory.soft_limit_in_bytes"), String.valueOf(maxJobRSSBytes), "UTF-8");
            Miscellaneous.writeStringToFile(new File(f, "memory.limit_in_bytes"), String.valueOf(Config.getInstance().getSchedulerCfg().getMaxJobRSSBytes()), "UTF-8");

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public CgroupMemoryStats getCgroupMemoryStats(int jobId) {
        File f = new File(Config.getInstance().getSchedulerCfg().getMemoryCgroupBasePath() + "/" + WavaHome.getInstance().getId() + "/" + String.valueOf(jobId) + "/memory.stat");
        try {
            String content = Miscellaneous.toString(new FileInputStream(f), "UTF-8");
            String[] lines = content.split("\n");
            CgroupMemoryStats ret = new CgroupMemoryStats();
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                String[] tokens = line.split("\\s+");
                if (tokens[0].equals("rss")) {
                    ret.rssBytes = Long.valueOf(tokens[1]);
                } else if (tokens[0].equals("swap")) {
                    ret.swapBytes = Long.valueOf(tokens[1]);
                    break;
                }
            }
            return ret;
        } catch (FileNotFoundException ex) {
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void removeJobMemoryCgroup(int jobId) {
        try {
            File f = new File(Config.getInstance().getSchedulerCfg().getMemoryCgroupBasePath() + "/" + WavaHome.getInstance().getId() + "/" + String.valueOf(jobId));
            removeLeafFolder(f);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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
    public TreeStats[] getTreeStats(int[] pIds) {
        TreeStats[] ret = new TreeStats[pIds.length];
        Map<Integer, Integer> indexes = new HashMap<>();
        for (int i = 0; i < pIds.length; i++) {
            indexes.put(pIds[i], i);
        }
        String[] cmd = {"ps", "axo", "pid,ppid,rss,%cpu", "--no-headers", "--sort=start_time"};
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
                    TreeStats st = ret[index];
                    if (st == null) {
                        st = new TreeStats();
                        ret[index] = st;
                    }
                    st.rssBytes += Long.valueOf(cols[2].trim()) * 1000;
                    st.cpuPercentage += Double.valueOf(cols[3].trim());
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
            sb.append("\"").append(cmd[i].replaceAll("\"", "\\\\\"")).append("\"");
        }
        return new String[]{"runuser", "-p", user, "-c", sb.toString()};
    }

    @Override
    public String[] decorateRunInCgroup(String[] cmd, int jobId) {
        File f = new File(Config.getInstance().getSchedulerCfg().getMemoryCgroupBasePath() + "/" + WavaHome.getInstance().getId() + "/" + jobId + "/cgroup.procs");
        StringBuilder sb = new StringBuilder("echo $$ >");
        sb.append(f.getAbsolutePath());
        sb.append(" && ");
        for (int i = 0; i < cmd.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append("\"").append(cmd[i].replaceAll("\"", "\\\\\"")).append("\"");
        }
        return new String[]{"/bin/bash", "-c", sb.toString()};
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

    public static void main(String[] args) {
        POSIXCommandsImpl pi = new POSIXCommandsImpl();
        String[] cmd = {"echo", "{\"refVersion\":\"human/19/GRCh37\",\"flankSize\":1000,\"targetSize\":40,\"productSizeRange\":\"175-275\",\"maxPrimerNumber\":5}"};
        String[] decorateRunAsCommand = pi.decorateRunAsCommand(cmd, "nacho");
        System.out.println(Arrays.toString(decorateRunAsCommand));
    }
}
