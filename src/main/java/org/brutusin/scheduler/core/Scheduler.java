package org.brutusin.scheduler.core;

import org.brutusin.scheduler.core.plug.PromiseHandler;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.scheduler.data.RequestInfo;
import org.brutusin.scheduler.data.Stats;

public class Scheduler {

    private final static Logger LOGGER = Logger.getLogger(Scheduler.class.getName());

    private final Map<Integer, JobInfo> jobMap = Collections.synchronizedMap(new HashMap<Integer, JobInfo>());
    private final Map<Integer, ProcessInfo> processMap = Collections.synchronizedMap(new HashMap<Integer, ProcessInfo>());
    private final NavigableSet<Key> jobQueue = Collections.synchronizedNavigableSet(new TreeSet());
    private final Map<Integer, GroupInfo> groupMap = Collections.synchronizedMap(new HashMap());
    private final AtomicInteger counter = new AtomicInteger();
    private final ThreadGroup threadGroup = new ThreadGroup(Scheduler.class.getName());
    private final Thread processingThread;
    private final Config cfg;

    private final String runningUser;
    private boolean closed;

    public Scheduler() throws IOException, InterruptedException {
        this(null);
    }

    public Scheduler(Config cfg) throws IOException, InterruptedException {
        if (cfg == null) {
            cfg = new Config();
        }
        this.cfg = cfg;
        this.runningUser = LinuxCommands.getInstance().getRunningUser();

        File f = new File(Environment.ROOT, "streams/");
        Miscellaneous.deleteDirectory(f);
        this.processingThread = new Thread(this.threadGroup, "processingThread") {
            @Override
            public void run() {
                while (true) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    try {
                        synchronized (jobQueue) {
                            try {
                                refresh();
                            } catch (IOException ex) {
                                LOGGER.log(Level.SEVERE, null, ex);
                            }
                        }
                        Thread.sleep(Scheduler.this.cfg.getPollingSecs() * 1000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }
        };
        this.processingThread.setDaemon(true);
        this.processingThread.start();
    }

    private String[] getPIds() {
        synchronized (processMap) {
            String[] ret = new String[processMap.size()];
            int i = 0;
            for (ProcessInfo pi : processMap.values()) {
                ret[i++] = pi.getPid();
            }
            return ret;
        }
    }

    private long getMaxPromisedMemory() {
        synchronized (processMap) {
            long sum = 0;
            for (Integer id : processMap.keySet()) {
                JobInfo ji = jobMap.get(id);
                sum += ji.getRequestInfo().getMaxRSS();
            }
            return sum;
        }
    }

    private void refresh() throws IOException, InterruptedException {

        long maxPromisedMemory = getMaxPromisedMemory();
        long availableMemory;
        if (cfg.getMaxTotalRSSBytes() > 0) {
            availableMemory = cfg.getMaxTotalRSSBytes() - maxPromisedMemory;
        } else {
            availableMemory = LinuxCommands.getInstance().getSystemRSSMemory() - maxPromisedMemory;
        }
        checkPromises(availableMemory);
        synchronized (jobQueue) {
            while (jobQueue.size() > 0) {
                final Key key = jobQueue.pollFirst();
                JobInfo ji = jobMap.get(key.getGlobalId());
                if (ji.getRequestInfo().getMaxRSS() > availableMemory) {
                    break;
                }
                jobMap.remove(key.getGlobalId());
                execute(ji);
                availableMemory -= ji.getRequestInfo().getMaxRSS();
            }
        }
    }

    private void checkPromises(long availableMemory) throws IOException, InterruptedException {
        String[] pIds = getPIds();
        if (pIds.length > 0) {
            Map<String, Stats> statMap = LinuxCommands.getInstance().getStats(pIds);
            for (Map.Entry<Integer, ProcessInfo> entry : processMap.entrySet()) {
                Integer globalId = entry.getKey();
                ProcessInfo pi = entry.getValue();
                Stats stats = statMap.get(pi.getPid());
                if (stats != null) {
                    JobInfo ji = jobMap.get(globalId);
                    if (ji.getRequestInfo().getMaxRSS() < stats.getRssBytes()) {
                        PromiseHandler.getInstance().promiseFailed(availableMemory, pi, stats);
                    }
                }
            }
        }
    }

    public Integer submit(String user, RequestInfo ri) throws IOException, InterruptedException {
        synchronized (jobQueue) {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            if (ri == null) {
                throw new IllegalArgumentException("Request info is required");
            }
            JobInfo ji = createJobInfo(user, ri);
            GroupInfo gi = getGroup(ri.getGroupId());
            gi.getJobs().add(ji.getId());
            Key key = new Key(gi.getPriority(), ri.getGroupId(), ji.getId());
            jobQueue.add(key);
            return ji.getId();
        }
    }

    private JobInfo createJobInfo(String user, RequestInfo ri) throws IOException, InterruptedException {
        int globalId = counter.getAndIncrement();
        JobInfo ji = new JobInfo(globalId, user, ri);
        jobMap.put(globalId, ji);
        return ji;
    }

    private GroupInfo getGroup(int groupId) {
        GroupInfo gi = groupMap.get(groupId);
        if (gi == null) {
            gi = new GroupInfo();
            gi.setGroupId(groupId);
            gi.setJobs(new HashSet<Integer>());
            groupMap.put(groupId, gi);
        }
        return gi;
    }

    public void setPriority(Integer groupId, Integer newPriority) {
        if (groupId == null) {
            throw new IllegalArgumentException("Group id is required");
        }
        if (newPriority == null) {
            throw new IllegalArgumentException("New priority is required");
        }
        synchronized (jobQueue) {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            GroupInfo gi = groupMap.get(groupId);
            if (gi == null) {
                throw new IllegalArgumentException("Unmanaged group id");
            }
            for (Integer id : gi.getJobs()) {
                Key key = new Key(gi.getPriority(), groupId, id);
                jobQueue.remove(key);
                Key newKey = new Key(newPriority, groupId, id);
                jobQueue.add(newKey);
            }
            gi.setPriority(newPriority);
        }
    }

    private static void writeSilently(OutputStream os, String message) {
        try {
            os.write(message.getBytes());
            os.write("\n".getBytes());
        } catch (IOException ex) {
            // Peer closed
        }
    }

    private void execute(final JobInfo ji) {
        if (ji == null) {
            throw new IllegalArgumentException("Id is required");
        }
        Thread t = new Thread(this.threadGroup, "scheduled process " + ji.getId()) {
            @Override
            public void run() {
                String[] cmd;
                if (Scheduler.this.runningUser.equals("root")) {
                    cmd = LinuxCommands.getInstance().getRunAsCommand(ji.getUser(), ji.getRequestInfo().getCommand());
                } else {
                    cmd = ji.getRequestInfo().getCommand();
                }
                ProcessBuilder pb = new ProcessBuilder(cmd);
                //pb.environment().clear();
                pb.directory(ji.getRequestInfo().getWorkingDirectory());
                if (ji.getRequestInfo().getEnvironment() != null) {
                    pb.environment().putAll(ji.getRequestInfo().getEnvironment());
                }
                Process process;
                writeSilently(ji.getLifeCycleOs(), "started");
                try {
                    process = pb.start();
                } catch (IOException ex) {
                    writeSilently(ji.getLifeCycleOs(), "scheduler-error: " + Miscellaneous.getStrackTrace(ex));
                    return;
                }
                Thread stoutReaderThread = Miscellaneous.pipeAsynchronously(process.getInputStream(), ji.getStdoutOs());
                Thread sterrReaderThread = Miscellaneous.pipeAsynchronously(process.getErrorStream(), ji.getStderrOs());
                try {
                    int code = process.waitFor();
                    writeSilently(ji.getLifeCycleOs(), "retcode: " + code);
                } catch (InterruptedException ex) {
                    stoutReaderThread.interrupt();
                    sterrReaderThread.interrupt();
                    process.destroy();
                } finally {
                    try {
                        ji.getLifeCycleOs().close();
                        ji.getStdoutOs().close();
                        ji.getStderrOs().close();
                        stoutReaderThread.join();
                        sterrReaderThread.join();
                    } catch (Throwable th) {
                        LOGGER.log(Level.SEVERE, th.getMessage());
                    }
                }
            }
        };
        t.start();
    }

    public void close() {
        synchronized (jobQueue) {
            this.closed = true;
            this.threadGroup.interrupt();
        }
    }

    public static class Key implements Comparable<Key> {

        private final int priority;
        private final int groupId;
        private final int globalId;

        public Key(int priority, int groupId, int globalId) {
            this.priority = priority;
            this.groupId = groupId;
            this.globalId = globalId;
        }

        public int getPriority() {
            return priority;
        }

        public int getGroupId() {
            return groupId;
        }

        public int getGlobalId() {
            return globalId;
        }

        @Override
        public int compareTo(Key o) {
            int ret = Integer.compare(priority, o.priority);
            if (ret == 0) {
                ret = Integer.compare(groupId, o.groupId);
                if (ret == 0) {
                    ret = Integer.compare(globalId, o.globalId);
                }
            }
            return ret;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Key)) {
                return false;
            }
            Key other = (Key) obj;
            return globalId == other.globalId;
        }

        @Override
        public int hashCode() {
            return globalId;
        }
    }

    private class GroupInfo {

        private int groupId;
        private String user;
        private Set<Integer> jobs;
        private int priority;

        public int getGroupId() {
            return groupId;
        }

        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public Set<Integer> getJobs() {
            return jobs;
        }

        public void setJobs(Set<Integer> jobs) {
            this.jobs = jobs;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }
    }

    public class JobInfo {

        private final int id;
        private final String user;
        private final File lifeCycleNamedPipe;
        private final File stdoutNamedPipe;
        private final File stderrNamedPipe;

        private final FileOutputStream lifeCycleOs;
        private final FileOutputStream stdoutOs;
        private final FileOutputStream stderrOs;

        private final RequestInfo requestInfo;

        public JobInfo(int id, String user, RequestInfo requestInfo) throws IOException, InterruptedException {
            this.id = id;
            this.user = user;
            this.requestInfo = requestInfo;
            this.lifeCycleNamedPipe = new File(Environment.ROOT, "streams/" + id + "/lifecycle");
            this.stdoutNamedPipe = new File(Environment.ROOT, "streams/" + id + "/stdout");
            this.stderrNamedPipe = new File(Environment.ROOT, "streams/" + id + "/stderr");
            LinuxCommands.getInstance().createNamedPipes(lifeCycleNamedPipe, stderrNamedPipe, stdoutNamedPipe);
            this.lifeCycleOs = new FileOutputStream(lifeCycleNamedPipe);
            this.stdoutOs = new FileOutputStream(stdoutNamedPipe);
            this.stderrOs = new FileOutputStream(stderrNamedPipe);
        }

        public int getId() {
            return id;
        }

        public String getUser() {
            return user;
        }

        public RequestInfo getRequestInfo() {
            return requestInfo;
        }

        public OutputStream getLifeCycleOs() {
            return lifeCycleOs;
        }

        public OutputStream getStdoutOs() {
            return stdoutOs;
        }

        public OutputStream getStderrOs() {
            return stderrOs;
        }
    }

    public class ProcessInfo {

        private String pid;
        private Process process;
        private JobInfo jobInfo;

        public JobInfo getJobInfo() {
            return jobInfo;
        }

        public void setJobInfo(JobInfo jobInfo) {
            this.jobInfo = jobInfo;
        }

        public Process getProcess() {
            return process;
        }

        public void setProcess(Process process) {
            this.process = process;
        }

        public String getPid() {
            return pid;
        }

        public void setPid(String pid) {
            this.pid = pid;
        }

    }

    public static class Config {

        private int pollingSecs = 10;
        private int maxTotalRSSBytes = -1;

        public Config() {
        }

        public int getPollingSecs() {
            return pollingSecs;
        }

        public int getMaxTotalRSSBytes() {
            return maxTotalRSSBytes;
        }

        public void setPollingSecs(int pollingSecs) {
            if (pollingSecs < 1) {
                throw new IllegalArgumentException("pollingSecs must be a positive integer");
            }
            this.pollingSecs = pollingSecs;
        }

        public void setMaxTotalRSSBytes(int maxTotalRSSBytes) {
            this.maxTotalRSSBytes = maxTotalRSSBytes;
        }

        public Config pollingSecs(int pollingSecs) {
            setPollingSecs(pollingSecs);
            return this;
        }

        public Config maxTotalRSSBytes(int maxTotalRSSBytes) {
            setMaxTotalRSSBytes(maxTotalRSSBytes);
            return this;
        }
    }
}
