package org.brutusin.scheduler.core;

import org.brutusin.scheduler.core.plug.PromiseHandler;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.scheduler.data.CancelInfo;
import org.brutusin.scheduler.data.SubmitInfo;
import org.brutusin.scheduler.data.Stats;

public class Scheduler {

    private final static Logger LOGGER = Logger.getLogger(Scheduler.class.getName());
    private final Map<Integer, PeerChannel<SubmitInfo>> jobMap = Collections.synchronizedMap(new HashMap<Integer, PeerChannel<SubmitInfo>>());
    private final Map<Integer, ProcessInfo> processMap = Collections.synchronizedMap(new HashMap<Integer, ProcessInfo>());
    private final Map<Integer, Integer> previousPositionMap = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private final NavigableSet<Key> jobQueue = Collections.synchronizedNavigableSet(new TreeSet());
    private final Map<Integer, GroupInfo> groupMap = Collections.synchronizedMap(new HashMap());
    private final ThreadGroup threadGroup = new ThreadGroup(Scheduler.class.getName());
    private final AtomicInteger counter = new AtomicInteger();
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

        remakeFolder(new File(Environment.ROOT, "streams/"));
        remakeFolder(new File(Environment.ROOT, "state/"));
        remakeFolder(new File(Environment.ROOT, "request/"));

        this.processingThread = new Thread(this.threadGroup, "processingThread") {
            @Override
            public void run() {
                while (true) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    try {
                        Thread.sleep(Scheduler.this.cfg.getPollingSecs() * 1000);
                        refresh();
                    } catch (Throwable th) {
                        LOGGER.log(Level.SEVERE, null, th);
                        if (th instanceof InterruptedException) {
                            break;
                        }
                    }
                }
            }
        };
        this.processingThread.setDaemon(true);
        this.processingThread.start();
    }

    private static void remakeFolder(File f) throws IOException {
        Miscellaneous.deleteDirectory(f);
        Miscellaneous.createDirectory(f);
    }

    private int[] getPIds() {
        synchronized (processMap) {
            int[] ret = new int[processMap.size()];
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
                PeerChannel<SubmitInfo> channel = processMap.get(id).getChannel();
                sum += channel.getRequest().getMaxRSS();
            }
            return sum;
        }
    }

    private void cleanStalePeers() throws InterruptedException {
        synchronized (jobQueue) {
            Iterator<Key> iterator = jobQueue.iterator();
            while (iterator.hasNext()) {
                Key key = iterator.next();
                PeerChannel<SubmitInfo> channel = jobMap.get(key.getId());
                if (!channel.sendLogToPeer(Event.ping, null)) {
                    iterator.remove();
                    jobMap.remove(key.getId());
                }
            }
        }
        synchronized (processMap) {
            for (ProcessInfo pi : processMap.values()) {
                if (!pi.getChannel().sendLogToPeer(Event.ping, null)) {
                    try {
                        LinuxCommands.getInstance().killTree(pi.getPid());
                    } catch (IOException ex) {
                        LOGGER.log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
    }

    private void refresh() throws IOException, InterruptedException {
        cleanStalePeers();
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
                final Key key = jobQueue.first();
                PeerChannel<SubmitInfo> channel = jobMap.get(key.getId());
                if (channel.getRequest().getMaxRSS() > availableMemory) {
                    break;
                }
                jobQueue.pollFirst();
                jobMap.remove(key.getId());
                execute(key.getId(), channel);
                availableMemory -= channel.getRequest().getMaxRSS();
            }
            int position = 0;
            for (Key key : jobQueue) {
                position++;
                PeerChannel channel = jobMap.get(key.getId());
                Integer prevPosition = previousPositionMap.get(key.getId());
                if (prevPosition == null || position != prevPosition) {
                    channel.sendLogToPeer(Event.info, "job enqueded at position " + position + " ...");
                    previousPositionMap.put(key.getId(), position);
                }
            }
        }
    }

    private void checkPromises(long availableMemory) throws IOException, InterruptedException {
        int[] pIds = getPIds();
        if (pIds.length > 0) {
            Map<String, Stats> statMap = LinuxCommands.getInstance().getStats(pIds);
            synchronized (processMap) {
                for (Map.Entry<Integer, ProcessInfo> entry : processMap.entrySet()) {
                    ProcessInfo pi = entry.getValue();
                    Stats stats = statMap.get(pi.getPid());
                    if (stats != null) {
                        PeerChannel<SubmitInfo> channel = pi.getChannel();
                        if (channel.getRequest().getMaxRSS() < stats.getRssBytes()) {
                            PromiseHandler.getInstance().promiseFailed(availableMemory, pi, stats);
                        }
                    }
                }
            }
        }
    }

    public void submit(PeerChannel<SubmitInfo> submitChannel) throws IOException, InterruptedException {
        synchronized (jobQueue) {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            if (submitChannel == null) {
                throw new IllegalArgumentException("Request info is required");
            }
            int id = counter.incrementAndGet();
            jobMap.put(id, submitChannel);
            submitChannel.sendLogToPeer(Event.id, String.valueOf(id));
            submitChannel.sendLogToPeer(Event.info, "command successfully received");
            GroupInfo gi = getGroup(submitChannel.getRequest().getGroupId());
            gi.getJobs().add(id);
            Key key = new Key(gi.getPriority(), submitChannel.getRequest().getGroupId(), id);
            jobQueue.add(key);
            refresh();
        }
    }

    public void cancel(PeerChannel<CancelInfo> cancelChannel) throws IOException, InterruptedException {
        synchronized (jobQueue) {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            PeerChannel<SubmitInfo> submitChannel = jobMap.remove(cancelChannel.getRequest().getId());
            if (submitChannel != null) {
                if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(submitChannel.getUser())) {
                    cancelChannel.sendLogToPeer(Event.error, "user '" + cancelChannel.getUser() + "' is not allowed to cancel a job from user '" + submitChannel.getUser() + "'");
                    cancelChannel.close();
                    return;
                }
                GroupInfo gi = groupMap.get(submitChannel.getRequest().getGroupId());
                Key key = new Key(gi.getPriority(), gi.groupId, cancelChannel.getRequest().getId());
                jobQueue.remove(key);
                submitChannel.sendLogToPeer(Event.interrupted, "Interrupted by user " + cancelChannel.getUser());
                submitChannel.sendLogToPeer(Event.retcode, "-1");
                submitChannel.close();
                cancelChannel.sendLogToPeer(Event.info, "enqueued job sucessfully cancelled");
                cancelChannel.close();
                return;
            }
        }
        ProcessInfo pi = processMap.get(cancelChannel.getRequest().getId());
        if (pi == null) {
            cancelChannel.sendLogToPeer(Event.error, "job #" + cancelChannel.getRequest().getId() + " not found");
            cancelChannel.close();
        } else {
            LinuxCommands.getInstance().killTree(pi.getPid());
            cancelChannel.sendLogToPeer(Event.info, "running job sucessfully cancelled");
            cancelChannel.close();
        }
    }

    private GroupInfo getGroup(int groupId) {
        synchronized (groupMap) {
            GroupInfo gi = groupMap.get(groupId);
            if (gi == null) {
                gi = new GroupInfo();
                gi.setGroupId(groupId);
                gi.setJobs(Collections.synchronizedSet(new HashSet<Integer>()));
                groupMap.put(groupId, gi);
            }
            return gi;
        }
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

    private void execute(final int id, final PeerChannel<SubmitInfo> channel) {
        if (channel == null) {
            throw new IllegalArgumentException("Id is required");
        }
        Thread t;
        t = new Thread(this.threadGroup, "scheduled process " + id) {
            @Override
            public void run() {
                String[] cmd;
                if (Scheduler.this.runningUser.equals("root")) {
                    cmd = LinuxCommands.getInstance().getRunAsCommand(channel.getUser(), channel.getRequest().getCommand());
                } else {
                    cmd = channel.getRequest().getCommand();
                }
                ProcessBuilder pb = new ProcessBuilder(cmd);
                //pb.environment().clear();
                pb.directory(channel.getRequest().getWorkingDirectory());
                if (channel.getRequest().getEnvironment() != null) {
                    pb.environment().putAll(channel.getRequest().getEnvironment());
                }
                Process process;
                int pId;
                try {
                    try {
                        process = pb.start();
                        pId = Miscellaneous.getUnixId(process);
                        channel.sendLogToPeer(Event.start, String.valueOf(pId));
                        ProcessInfo pi = new ProcessInfo(pId, channel);
                        processMap.put(id, pi);
                    } catch (IOException ex) {
                        channel.sendLogToPeer(Event.error, Miscellaneous.getStrackTrace(ex));
                        return;
                    }
                    Thread stoutReaderThread = Miscellaneous.pipeAsynchronously(process.getInputStream(), channel.getStdoutOs());
                    stoutReaderThread.setName("stdout-pid-" + pId);
                    Thread sterrReaderThread = Miscellaneous.pipeAsynchronously(process.getErrorStream(), channel.getStderrOs());
                    sterrReaderThread.setName("stderr-pid-" + pId);
                    try {
                        int code = process.waitFor();
                        channel.sendLogToPeer(Event.retcode, String.valueOf(code));
                    } catch (InterruptedException ex) {
                        try {
                            LinuxCommands.getInstance().killTree(pId);
                        } catch (Throwable th) {
                            LOGGER.log(Level.SEVERE, th.getMessage());
                        }
//                        stoutReaderThread.interrupt();
//                        sterrReaderThread.interrupt();
//                        process.destroy();
//                        channel.sendLogToPeer(Event.interrupted, ex.getMessage());
//                        return;
                    } finally {
                        try {
                            stoutReaderThread.join();
                            sterrReaderThread.join();
                        } catch (Throwable th) {
                            LOGGER.log(Level.SEVERE, th.getMessage());
                        }
                    }
                } finally {
                    try {
                        channel.close();
                        jobMap.remove(id);
                        processMap.remove(id);
                        GroupInfo group = groupMap.get(channel.getRequest().getGroupId());
                        group.getJobs().remove(id);
                        synchronized (groupMap) {
                            if (group.getJobs().isEmpty()) {
                                groupMap.remove(channel.getRequest().getGroupId());
                            }
                        }
                        refresh();
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
        private final int id;

        public Key(int priority, int groupId, int id) {
            this.priority = priority;
            this.groupId = groupId;
            this.id = id;
        }

        public int getPriority() {
            return priority;
        }

        public int getGroupId() {
            return groupId;
        }

        public int getId() {
            return id;
        }

        @Override
        public int compareTo(Key o) {
            int ret = Integer.compare(priority, o.priority);
            if (ret == 0) {
                ret = Integer.compare(groupId, o.groupId);
                if (ret == 0) {
                    ret = Integer.compare(id, o.id);
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
            return id == other.id;
        }

        @Override
        public int hashCode() {
            return id;
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

    public class ProcessInfo {

        private final int pId;
        private final PeerChannel<SubmitInfo> channel;

        public ProcessInfo(int pId, PeerChannel<SubmitInfo> channel) {
            this.pId = pId;
            this.channel = channel;
        }

        public PeerChannel getChannel() {
            return channel;
        }

        public int getPid() {
            return pId;
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
