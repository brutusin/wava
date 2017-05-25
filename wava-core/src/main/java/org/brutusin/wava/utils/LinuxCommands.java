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
package org.brutusin.wava.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.commons.utils.ProcessException;
import org.brutusin.commons.utils.ProcessUtils;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.core.stats.CpuStats;
import org.brutusin.wava.core.stats.IOStats;
import org.brutusin.wava.core.stats.MemoryStats;
import org.brutusin.wava.env.WavaHome;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class LinuxCommands {

    private static final Logger LOGGER = Logger.getLogger(LinuxCommands.class.getName());
    private static final File FILE_MEMINFO = new File("/proc/meminfo");
    private static final File MEMORY_CGROUP_ROOT = new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/memory/" + WavaHome.getInstance().getId());
    private static final File CPUACCT_CGROUP_ROOT = new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/cpuacct/" + WavaHome.getInstance().getId());
    private static final File BLKIO_CGROUP_ROOT = new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/blkio/" + WavaHome.getInstance().getId());

    private static String executeBashCommand(String command) throws ProcessException, InterruptedException {
        String[] cmd = {"/bin/bash", "-c", command};
        return ProcessUtils.executeProcess(cmd);
    }

    public static boolean createWavaCgroups(long totalManagedRss) {
        try {
            createMemoryCgroup(totalManagedRss);
            createCpuCgroup();
            createIOCgroup();
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static void createMemoryCgroup(long totalManagedRss) throws Exception {
        removeLeafFolder(MEMORY_CGROUP_ROOT);
        String[] cmd = {"mkdir", MEMORY_CGROUP_ROOT.getAbsolutePath()};
        ProcessUtils.executeProcess(cmd);
        Miscellaneous.writeStringToFile(new File(MEMORY_CGROUP_ROOT, "memory.limit_in_bytes"), String.valueOf(totalManagedRss), "UTF-8");
        long maxTotalSwapBytes = Miscellaneous.parseHumanReadableByteCount(Config.getInstance().getSchedulerCfg().getMaxSwap());
        if (maxTotalSwapBytes > 0) {
            File swapLimitFile = new File(MEMORY_CGROUP_ROOT, "memory.memsw.limit_in_bytes");
            if (swapLimitFile.exists()) {
                Miscellaneous.writeStringToFile(swapLimitFile, String.valueOf(totalManagedRss + maxTotalSwapBytes), "UTF-8");
            } else {
                LOGGER.warning("Swap limit is not enabled");
            }
        }
        Miscellaneous.writeStringToFile(new File(MEMORY_CGROUP_ROOT, "memory.oom_control"), Config.getInstance().getSchedulerCfg().isOutOfMemoryKillerEnabled() ? "0" : "1", "UTF-8");
        Miscellaneous.writeStringToFile(new File(MEMORY_CGROUP_ROOT, "memory.use_hierarchy"), "1", "UTF-8");
    }

    private static void createCpuCgroup() throws Exception {
        removeLeafFolder(CPUACCT_CGROUP_ROOT);
        String[] cmd = {"mkdir", CPUACCT_CGROUP_ROOT.getAbsolutePath()};
        ProcessUtils.executeProcess(cmd);
    }

    private static void createIOCgroup() throws Exception {
        removeLeafFolder(BLKIO_CGROUP_ROOT);
        String[] cmd = {"mkdir", BLKIO_CGROUP_ROOT.getAbsolutePath()};
        ProcessUtils.executeProcess(cmd);
    }

    private static void removeLeafFolder(File folder) throws ProcessException, InterruptedException {
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

    public static void createJobCgroups(int jobId, long maxJobRSSBytes) {
        createJobMemoryCgroup(jobId, maxJobRSSBytes);
        createJobCpuCgroup(jobId);
        createJobIOCgroup(jobId);
    }

    private static void createJobMemoryCgroup(int jobId, long maxJobRSSBytes) {
        try {
            File f = new File(MEMORY_CGROUP_ROOT, String.valueOf(jobId));
            String[] cmd = {"mkdir", f.getAbsolutePath()};
            ProcessUtils.executeProcess(cmd);
            Miscellaneous.writeStringToFile(new File(f, "memory.soft_limit_in_bytes"), String.valueOf(maxJobRSSBytes), "UTF-8");
            Miscellaneous.writeStringToFile(new File(f, "memory.limit_in_bytes"), String.valueOf(Miscellaneous.parseHumanReadableByteCount(Config.getInstance().getSchedulerCfg().getMaxJobSize())), "UTF-8");

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void createJobCpuCgroup(int jobId) {
        try {
            File f = new File(CPUACCT_CGROUP_ROOT, String.valueOf(jobId));
            String[] cmd = {"mkdir", f.getAbsolutePath()};
            ProcessUtils.executeProcess(cmd);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void createJobIOCgroup(int jobId) {
        try {
            File f = new File(BLKIO_CGROUP_ROOT, String.valueOf(jobId));
            String[] cmd = {"mkdir", f.getAbsolutePath()};
            ProcessUtils.executeProcess(cmd);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static MemoryStats getCgroupMemoryStats(Integer jobId) {
        File f;
        if (jobId == null) {
            f = new File(MEMORY_CGROUP_ROOT, "/memory.stat");
        } else {
            f = new File(MEMORY_CGROUP_ROOT, String.valueOf(jobId) + "/memory.stat");
        }
        try {
            long nanos1 = System.nanoTime();
            String content = Miscellaneous.toString(new FileInputStream(f), "UTF-8");
            String[] lines = content.split("\n");
            MemoryStats ret = new MemoryStats();
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
            long nanos2 = System.nanoTime();
            //  if elasped less than a ms (discard wrong timings due to garbage collection)
            if (nanos2 < nanos1 + 1000000) {
                ret.nanos = nanos2;
            }
            return ret;
        } catch (FileNotFoundException ex) {
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static int getUserHz() {
        try {
            return Integer.valueOf(ProcessUtils.executeProcess("getconf", "CLK_TCK"));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static CpuStats getCgroupCpuStats(Integer jobId) {
        CpuStats ret = new CpuStats();
        File f;
        if (jobId == null) {
            f = new File(CPUACCT_CGROUP_ROOT, "/cpuacct.stat");
        } else {
            f = new File(CPUACCT_CGROUP_ROOT, String.valueOf(jobId) + "/cpuacct.stat");
        }
        try {
            long nanos1 = System.nanoTime();
            String content = Miscellaneous.toString(new FileInputStream(f), "UTF-8");
            Scanner sc = new Scanner(content);
            sc.next();
            ret.userJiffies = Long.valueOf(sc.next());
            sc.next();
            ret.systemJiffies = Long.valueOf(sc.next());
            long nanos2 = System.nanoTime();
            //  if elasped less than a ms (discard wrong timings due to garbage collection)
            if (nanos2 < nanos1 + 1000000) {
                ret.nanos = nanos2;
            }
            return ret;
        } catch (FileNotFoundException ex) {
            return null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static IOStats getCgroupIOStats(Integer jobId) {

        File f;
        if (jobId == null) {
            f = new File(BLKIO_CGROUP_ROOT, "/blkio.throttle.io_service_bytes");
        } else {
            f = new File(BLKIO_CGROUP_ROOT, String.valueOf(jobId) + "/blkio.throttle.io_service_bytes");
        }
        try {
            long nanos1 = System.nanoTime();
            Scanner sc = new Scanner(f);
            String line = null;
            while (sc.hasNextLine()) {
                line = sc.nextLine();
            }
            if (line != null) {
                IOStats ret = new IOStats();
                ret.ioBytes = Long.valueOf(line.split("\\s+")[1]);
                long nanos2 = System.nanoTime();
                //  if elasped less than a ms (discard wrong timings due to garbage collection)
                if (nanos2 < nanos1 + 1000000) {
                    ret.nanos = nanos2;
                }
                return ret;
            }
            return null;
        } catch (FileNotFoundException ex) {
            return null;
        }
    }

    public static void removeJobCgroups(int jobId) {
        try {
            File memCgroup = new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/memory/" + WavaHome.getInstance().getId() + "/" + String.valueOf(jobId));
            executeBashCommand("echo 0 > " + new File(memCgroup, "memory.force_empty").getAbsolutePath());
            removeLeafFolder(memCgroup);
            removeLeafFolder(new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/cpuacct/" + WavaHome.getInstance().getId() + "/" + String.valueOf(jobId)));
            removeLeafFolder(new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/blkio/" + WavaHome.getInstance().getId() + "/" + String.valueOf(jobId)));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void setNiceness(int pId, int niceness) {
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

    public static void killTree(int pId) {
        List<Integer> visitedIds = new ArrayList<>();
        getTree(visitedIds, pId, true);
        sendSignal(visitedIds, 9); // SIGKILL
    }

    private static void getTree(List<Integer> visited, int pId, boolean stop) {
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

    private static void sendSignal(List<Integer> pIds, int signal) {
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

//    public static TreeStats[] getTreeStats(int[] pIds) {
//        TreeStats[] ret = new TreeStats[pIds.length];
//        Map<Integer, Integer> indexes = new HashMap<>();
//        for (int i = 0; i < pIds.length; i++) {
//            indexes.put(pIds[i], i);
//        }
//        String[] cmd = {"ps", "axo", "pid,ppid,rss,%cpu", "--no-headers", "--sort=start_time"};
//        String stdout;
//        try {
//            stdout = ProcessUtils.executeProcess(cmd);
//        } catch (ProcessException ex) {
//            // no processes exist retcode=1
//            return null;
//        } catch (InterruptedException ex) {
//            throw new RuntimeException(ex);
//        }
//        if (stdout != null) {
//            String[] lines = stdout.split("\n");
//            for (String line : lines) {
//                String[] cols = line.trim().split("\\s+");
//                Integer pid = Integer.valueOf(cols[0].trim());
//                Integer index = indexes.get(pid);
//                if (index == null) {
//                    Integer ppid = Integer.valueOf(cols[1].trim());
//                    index = indexes.get(ppid);
//                    if (index != null) {
//                        indexes.put(pid, index);
//                    }
//                }
//                if (index != null) {
//                    TreeStats st = ret[index];
//                    if (st == null) {
//                        st = new TreeStats();
//                        ret[index] = st;
//                    }
//                    st.rssBytes += Long.valueOf(cols[2].trim()) * 1000;
//                    st.cpuPercentage += Double.valueOf(cols[3].trim());
//                }
//            }
//        }
//        return ret;
//    }
    public static String[] decorateWithCPUAffinity(String[] cmd, String affinity) {
        String[] ret = new String[cmd.length + 3];
        ret[0] = "taskset";
        ret[1] = "-c";
        ret[2] = affinity;
        for (int i = 3; i < ret.length; i++) {
            ret[i] = cmd[i - 3];
        }
        return ret;
    }

    public static String[] decorateWithBatchSchedulerPolicy(String[] cmd) {
        String[] ret = new String[cmd.length + 3];
        ret[0] = "chrt";
        ret[1] = "-b";
        ret[2] = "0";
        for (int i = 3; i < ret.length; i++) {
            ret[i] = cmd[i - 3];
        }
        return ret;
    }

    public static long[] getMemInfo() {
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

    public static String[] decorateRunAsCommand(String[] cmd, String user) {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < cmd.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append("\"").append(cmd[i].replaceAll("\"", "\\\\\"")).append("\"");
        }
        return new String[]{"runuser", "-p", user, "-c", sb.toString()};
    }

    public static String[] decorateRunInCgroup(String[] cmd, int jobId) {
        StringBuilder sb = new StringBuilder("echo $$ >");
        sb.append(new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/memory/" + WavaHome.getInstance().getId() + "/" + jobId + "/cgroup.procs").getAbsolutePath());
        sb.append(" && ");
        sb.append("echo $$ >");
        sb.append(new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/cpuacct/" + WavaHome.getInstance().getId() + "/" + jobId + "/cgroup.procs").getAbsolutePath());
        sb.append(" && ");
        sb.append("echo $$ >");
        sb.append(new File(Config.getInstance().getSchedulerCfg().getCgroupRootPath() + "/blkio/" + WavaHome.getInstance().getId() + "/" + jobId + "/cgroup.procs").getAbsolutePath());
        sb.append(" && ");
        for (int i = 0; i < cmd.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append("\"").append(cmd[i].replaceAll("\"", "\\\\\"")).append("\"");
        }
        return new String[]{"/bin/bash", "-c", sb.toString()};
    }

    public static String getRunningUser() {
        try {
            String[] cmd = {"id", "-un"};
            return ProcessUtils.executeProcess(cmd);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String getFileOwner(File f) {
        try {
            return executeBashCommand("ls -ld \"" + f.getAbsolutePath() + "\" | awk 'NR==1 {print $3}'");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) {
        LinuxCommands commands = new LinuxCommands();
        String[] cmd = {"echo", "{\"refVersion\":\"human/19/GRCh37\",\"flankSize\":1000,\"targetSize\":40,\"productSizeRange\":\"175-275\",\"maxPrimerNumber\":5}"};
        String[] decorateRunAsCommand = commands.decorateRunAsCommand(cmd, "nacho");
        System.out.println(Arrays.toString(decorateRunAsCommand));
    }
}
