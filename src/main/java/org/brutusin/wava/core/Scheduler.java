package org.brutusin.wava.core;

import org.brutusin.wava.core.cfg.Config;
import org.brutusin.wava.core.plug.PromiseHandler;
import org.brutusin.wava.core.plug.LinuxCommands;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.brutusin.commons.utils.ErrorHandler;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.SubmitInput;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.utils.Utils;

public class Scheduler {

    public final static String DEFAULT_GROUP_NAME = "default";
    public final static int EVICTION_ETERNAL = -1;

    private final static Logger LOGGER = Logger.getLogger(Scheduler.class.getName());
    private final static int NICENESS_RANGE = Config.getInstance().getNicenessRange()[1] - Config.getInstance().getNicenessRange()[0];
    private final static long MAX_MANAGED_RSS;

    static {
        if (Config.getInstance().getMaxTotalRSSBytes() > 0) {
            MAX_MANAGED_RSS = Config.getInstance().getMaxTotalRSSBytes();
        } else {
            try {
                MAX_MANAGED_RSS = LinuxCommands.getInstance().getSystemRSSMemory();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private final Map<Integer, PeerChannel<SubmitInput>> jobMap = Collections.synchronizedMap(new HashMap<Integer, PeerChannel<SubmitInput>>());
    private final Map<Key, ProcessInfo> processMap = Collections.synchronizedMap(new TreeMap<Key, ProcessInfo>());
    private final Map<Integer, Integer> previousPositionMap = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private final NavigableSet<Key> jobQueue = Collections.synchronizedNavigableSet(new TreeSet());
    private final Map<String, GroupInfo> groupMap = Collections.synchronizedMap(new HashMap());
    private final ThreadGroup threadGroup = new ThreadGroup(Scheduler.class.getName());
    private final AtomicInteger jobCounter = new AtomicInteger();
    private final AtomicInteger groupCounter = new AtomicInteger();
    private final Thread processingThread;

    private final String runningUser;
    private boolean closed;

    public Scheduler() throws IOException, InterruptedException {
        this.runningUser = LinuxCommands.getInstance().getRunningUser();
        createGroupInfo(DEFAULT_GROUP_NAME, LinuxCommands.getInstance().getRunningUser(), 0, EVICTION_ETERNAL);
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
                        Thread.sleep(Config.getInstance().getPollingSecs() * 1000);
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

    private GroupInfo createGroupInfo(String name, String user, int priority, int timetoIdleSeconds) {
        synchronized (groupMap) {
            if (!groupMap.containsKey(name)) {
                GroupInfo gi = new GroupInfo(name, user, timetoIdleSeconds);
                gi.setPriority(priority);
                groupMap.put(gi.getGroupName(), gi);
                return gi;
            }
        }
        return null;
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
            for (ProcessInfo pi : processMap.values()) {
                sum += pi.getChannel().getRequest().getMaxRSS();
            }
            return sum;
        }
    }

    private void cleanStalePeers() throws InterruptedException {
        synchronized (jobQueue) {
            Iterator<Key> iterator = jobQueue.iterator();
            while (iterator.hasNext()) {
                Key key = iterator.next();
                PeerChannel<SubmitInput> channel = jobMap.get(key.getId());
                if (!channel.ping()) {
                    iterator.remove();
                    jobMap.remove(key.getId());
                }
            }
        }
        synchronized (processMap) {
            for (ProcessInfo pi : processMap.values()) {
                if (!pi.getChannel().ping()) {
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
        updateNiceness();
        long availableMemory = MAX_MANAGED_RSS - getMaxPromisedMemory();
        checkPromises(availableMemory);
        long freeRSS = LinuxCommands.getInstance().getSystemRSSFreeMemory();
        if (availableMemory > freeRSS) {
            availableMemory = freeRSS;
        }
        synchronized (jobQueue) {
            while (jobQueue.size() > 0) {
                final Key key = jobQueue.first();
                PeerChannel<SubmitInput> channel = jobMap.get(key.getId());
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
                PeerChannel<SubmitInput> channel = jobMap.get(key.getId());
                Integer prevPosition = previousPositionMap.get(key.getId());
                if (prevPosition == null || position != prevPosition) {
                    channel.sendEvent(Event.queued, position);
                    previousPositionMap.put(key.getId(), position);
                }
            }
        }
    }

    private void updateNiceness() throws IOException, InterruptedException {
        updateNiceness(null);
    }

    private void updateNiceness(Integer pId) throws IOException, InterruptedException {
        synchronized (processMap) {
            int i = 0;
            for (ProcessInfo pi : processMap.values()) {
                if (pId == null || pi.getPid() == pId) {
                    pi.setNiceness(Config.getInstance().getNicenessRange()[0] + (i * NICENESS_RANGE) / (processMap.size() > 1 ? (processMap.size() - 1) : 1));
                }
                i++;
            }
        }
    }

    private long checkPromises(long availableMemory) throws IOException, InterruptedException {

        long currentRSS = 0;
        synchronized (processMap) {
            int[] pIds = getPIds();
            if (pIds.length > 0) {
                long[] treeRSSs = LinuxCommands.getInstance().getTreeRSS(pIds);
                if (treeRSSs != null) {
                    int i = 0;
                    for (ProcessInfo pi : processMap.values()) {
                        long treeRSS = treeRSSs[i++];
                        currentRSS += treeRSS;
                        if (treeRSS != 0) {
                            PeerChannel<SubmitInput> channel = pi.getChannel();
                            if (treeRSS > pi.getMaxSeenRSS()) {
                                pi.setMaxSeenRSS(treeRSS);
                            }
                            if (channel.getRequest().getMaxRSS() < treeRSS) {
                                PromiseHandler.getInstance().promiseFailed(availableMemory, pi, treeRSS);
                            }
                        }
                    }
                }
            }
        }
        return currentRSS;
    }

    public void submit(PeerChannel<SubmitInput> submitChannel) throws IOException, InterruptedException {
        synchronized (jobQueue) {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            if (submitChannel == null) {
                throw new IllegalArgumentException("Request info is required");
            }
            if (submitChannel.getRequest().getGroupName() == null) {
                submitChannel.getRequest().setGroupName(DEFAULT_GROUP_NAME);
            }
            int id = jobCounter.incrementAndGet();
            GroupInfo gi;
            synchronized (groupMap) {
                gi = groupMap.get(submitChannel.getRequest().getGroupName());
                if (gi == null) { // dynamic group
                    gi = createGroupInfo(submitChannel.getRequest().getGroupName(), submitChannel.getUser(), 0, Config.getInstance().getDynamicGroupIdleSeconds());
                }
                gi.getJobs().add(id);
            }
            jobMap.put(id, submitChannel);
            submitChannel.sendEvent(Event.id, id);
            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
            jobQueue.add(key);
            submitChannel.sendEvent(Event.priority, gi.getPriority());
            refresh();
        }
    }

    public void listJobs(PeerChannel<Void> channel) throws IOException, InterruptedException {
        try {
            StringBuilder header = new StringBuilder(ANSICode.CLEAR.getCode());
            header.append(ANSICode.MOVE_TO_TOP.getCode());
            header.append(ANSICode.BLACK.getCode());
            header.append(ANSICode.BG_GREEN.getCode());
            header.append(StringUtils.leftPad("JOB_ID", 8));
            header.append(" ");
            header.append(StringUtils.rightPad("GROUP", 8));
            header.append(" ");
            header.append(StringUtils.rightPad("USER", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("PRIORITY", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("QUEUE", 5));
            header.append(" ");
            header.append(StringUtils.leftPad("PID", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("NICE", 4));
            header.append(" ");
            header.append(StringUtils.leftPad("MAX_EXP_RSS", 12));
            header.append(" ");
            header.append(StringUtils.leftPad("MAX_SEEN_RSS", 12));
            header.append(" ");
            header.append("CMD");
            header.append(ANSICode.END_OF_LINE.getCode());
            header.append(ANSICode.RESET.getCode());
            PeerChannel.println(channel.getStdoutOs(), header.toString());
            synchronized (jobQueue) {
                synchronized (processMap) {
                    for (ProcessInfo pi : processMap.values()) {
                        GroupInfo gi = groupMap.get(pi.getChannel().getRequest().getGroupName());
                        StringBuilder line = new StringBuilder();
                        line.append(StringUtils.leftPad(String.valueOf(pi.getId()), 8));
                        line.append(" ");
                        line.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                        line.append(" ");
                        line.append(StringUtils.rightPad(pi.getChannel().getUser(), 8));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(gi.getPriority()), 8));
                        line.append(" ");
                        line.append(StringUtils.leftPad("", 5));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(pi.getPid()), 8));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(pi.getNiceness()), 4));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(pi.getChannel().getRequest().getMaxRSS()), 12));
                        line.append(" ");
                        if (pi.getMaxSeenRSS() > 0.9 * pi.getChannel().getRequest().getMaxRSS()) {
                            line.append(ANSICode.RED.getCode());
                        }
                        line.append(StringUtils.leftPad(String.valueOf(pi.getMaxSeenRSS()), 12));
                        line.append(ANSICode.RESET.getCode());
                        line.append(" ");
                        line.append(Arrays.toString(pi.getChannel().getRequest().getCommand()));
                        line.append(" ");
                        PeerChannel.println(channel.getStdoutOs(), line.toString());
                    }
                }
                int position = 0;
                for (Key key : jobQueue) {
                    position++;
                    PeerChannel<SubmitInput> submitChannel = jobMap.get(key.getId());
                    StringBuilder line = new StringBuilder();
                    line.append(ANSICode.YELLOW.getCode());
                    line.append(StringUtils.leftPad(String.valueOf(key.getId()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(String.valueOf(submitChannel.getRequest().getGroupName()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(submitChannel.getUser(), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(key.getPriority()), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(position), 5));
                    line.append(" ");
                    line.append(StringUtils.leftPad("", 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad("", 4));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(submitChannel.getRequest().getMaxRSS()), 12));
                    line.append(" ");
                    line.append(StringUtils.leftPad("", 12));
                    line.append(" ");
                    line.append(Arrays.toString(submitChannel.getRequest().getCommand()));
                    line.append(" ");
                    line.append(ANSICode.RESET.getCode());
                    PeerChannel.println(channel.getStdoutOs(), line.toString());
                }
            }
        } finally {
            channel.sendEvent(Event.retcode, 0);
            channel.close();
        }
    }

    public void cancel(PeerChannel<CancelInput> cancelChannel) throws IOException, InterruptedException {
        try {
            synchronized (jobQueue) {
                if (closed) {
                    throw new IllegalStateException("Instance is closed");
                }
                PeerChannel<SubmitInput> submitChannel = jobMap.get(cancelChannel.getRequest().getId());
                if (submitChannel != null) {
                    if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(submitChannel.getUser())) {
                        cancelChannel.log(ANSICode.RED, "user '" + cancelChannel.getUser() + "' is not allowed to cancel a queued job from user '" + submitChannel.getUser() + "'");
                        cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                        return;
                    }
                    jobMap.remove(cancelChannel.getRequest().getId());
                    GroupInfo gi = groupMap.get(submitChannel.getRequest().getGroupName());
                    Key key = new Key(gi.getPriority(), gi.groupId, cancelChannel.getRequest().getId());
                    jobQueue.remove(key);
                    submitChannel.log(ANSICode.YELLOW, "Cancelled by user '" + cancelChannel.getUser() + "'");
                    submitChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    submitChannel.close();
                    cancelChannel.log(ANSICode.GREEN, "enqueued job sucessfully cancelled");
                    cancelChannel.sendEvent(Event.retcode, 0);
                    return;
                }
            }
            ProcessInfo pi = processMap.get(cancelChannel.getRequest().getId());
            if (pi != null) {
                if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(pi.getChannel().getUser())) {
                    cancelChannel.log(ANSICode.RED, "user '" + cancelChannel.getUser() + "' is not allowed to terminate a running job from user '" + pi.getChannel().getUser() + "'");
                    cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    return;
                }
                LinuxCommands.getInstance().killTree(pi.getPid());
                cancelChannel.log(ANSICode.GREEN, "running job sucessfully cancelled");
                cancelChannel.sendEvent(Event.retcode, 0);
            } else {
                cancelChannel.log(ANSICode.RED, "job #" + cancelChannel.getRequest().getId() + " not found");
                cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
            }
        } finally {
            cancelChannel.close();
        }
    }

    public void updateGroup(PeerChannel<GroupInput> channel) throws IOException {
        try {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            GroupInfo gi;
            synchronized (groupMap) {
                gi = groupMap.get(channel.getRequest().getGroupName());
                if (gi == null) {
                    createGroupInfo(channel.getRequest().getGroupName(), channel.getUser(), channel.getRequest().getPriority(), channel.getRequest().getTimetoIdleSeconds());
                    channel.log(ANSICode.GREEN, "Group '" + channel.getRequest().getGroupName() + "' created successfully");
                    channel.sendEvent(Event.retcode, 0);
                    return;
                } else if (channel.getRequest().isDelete()) {
                    if (gi.getJobs().isEmpty()) {
                        channel.log(ANSICode.GREEN, "Group '" + channel.getRequest().getGroupName() + "' deleted successfully");
                        groupMap.remove(channel.getRequest().getGroupName());
                        channel.sendEvent(Event.retcode, 0);
                        return;
                    } else {
                        channel.log(ANSICode.RED, "Group '" + channel.getRequest().getGroupName() + "' cannot be deleted, since it contains " + gi.getJobs().size() + " active jobs");
                        channel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                        return;
                    }
                }
            }
            synchronized (gi) {
                Integer newPriority = channel.getRequest().getPriority();

                if (newPriority != null && newPriority != gi.getPriority()) {
                    synchronized (gi.getJobs()) {
                        for (Integer id : gi.getJobs()) {
                            PeerChannel<SubmitInput> submitChannel = null;
                            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                            Key newKey = new Key(newPriority, gi.getGroupId(), id);
                            boolean enqueded;
                            synchronized (jobQueue) {
                                enqueded = jobQueue.remove(key);
                                if (enqueded) {
                                    submitChannel = jobMap.get(id);
                                    jobQueue.add(newKey);
                                }
                            }
                            if (!enqueded) {
                                synchronized (processMap) {
                                    ProcessInfo pi = processMap.remove(key);
                                    if (pi != null) {
                                        processMap.put(newKey, pi);
                                        submitChannel = pi.getChannel();
                                    }
                                }
                            }
                            if (submitChannel != null) {
                                submitChannel.sendEvent(Event.priority, newPriority);
                            }
                        }
                    }
                    gi.setPriority(newPriority);
                    channel.log(ANSICode.GREEN, "Group '" + channel.getRequest().getGroupName() + "' priority updated successfully");
                }
                Integer newTimetoIdleSeconds = channel.getRequest().getTimetoIdleSeconds();
                if (newTimetoIdleSeconds != null && newTimetoIdleSeconds != gi.getTimeToIdelSeconds()) {
                    gi.setTimeToIdelSeconds(newTimetoIdleSeconds);
                    channel.log(ANSICode.GREEN, "Group '" + channel.getRequest().getGroupName() + "' time-to-idle updated successfully");
                }
                channel.sendEvent(Event.retcode, 0);
            }
        } finally {
            channel.close();
        }
    }

    private void execute(final int id, final PeerChannel<SubmitInput> channel) {
        if (channel == null) {
            throw new IllegalArgumentException("Id is required");
        }
        Thread t;
        t = new Thread(this.threadGroup, "scheduled process " + id) {
            @Override
            public void run() {
                GroupInfo gi = groupMap.get(channel.getRequest().getGroupName());
                String[] cmd = channel.getRequest().getCommand();
                if (Scheduler.this.runningUser.equals("root")) {
                    cmd = LinuxCommands.getInstance().getRunAsCommand(channel.getUser(), cmd);
                }
                cmd = LinuxCommands.getInstance().decorateWithCPUAffinity(cmd, Config.getInstance().getCpuAfinity());
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.environment().clear();
                pb.directory(channel.getRequest().getWorkingDirectory());
                if (channel.getRequest().getEnvironment() != null) {
                    pb.environment().putAll(channel.getRequest().getEnvironment());
                }
                ProcessInfo pi;
                Process process;
                int pId;
                try {
                    try {
                        process = pb.start();
                        pId = Miscellaneous.getUnixId(process);
                        channel.sendEvent(Event.running, pId);
                        pi = new ProcessInfo(id, pId, channel);
                        synchronized (gi) {
                            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                            processMap.put(key, pi);
                        }
                        updateNiceness(pId);
                    } catch (Exception ex) {
                        channel.sendEvent(Event.error, JsonCodec.getInstance().transform(Miscellaneous.getStrackTrace(ex)));
                        channel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                        return;
                    }
                    Thread stoutReaderThread = Miscellaneous.pipeAsynchronously(process.getInputStream(), (ErrorHandler) null, true, channel.getStdoutOs());
                    stoutReaderThread.setName("stdout-pid-" + pId);
                    Thread sterrReaderThread = Miscellaneous.pipeAsynchronously(process.getErrorStream(), (ErrorHandler) null, true, channel.getStderrOs());
                    sterrReaderThread.setName("stderr-pid-" + pId);
                    try {
                        int code = process.waitFor();
                        channel.sendEvent(Event.maxrss, pi.getMaxSeenRSS());
                        channel.sendEvent(Event.retcode, code);
                    } catch (InterruptedException ex) {
                        try {
                            LinuxCommands.getInstance().killTree(pId);
                        } catch (Throwable th) {
                            LOGGER.log(Level.SEVERE, th.getMessage());
                        }
//                        stoutReaderThread.interrupt();
//                        sterrReaderThread.interrupt();
//                        process.destroy();
//                        channel.log(Event.interrupted, ex.getMessage());
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
                        synchronized (gi) {
                            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                            processMap.remove(key);
                        }
                        gi.getJobs().remove(id);
                        synchronized (groupMap) {
                            if (gi.getJobs().isEmpty()) {
                                if (gi.getTimeToIdelSeconds() == 0) {
                                    groupMap.remove(gi.getGroupName());
                                } else if (gi.getTimeToIdelSeconds() > 0) {
                                    Thread t = new Thread(threadGroup, "group-" + gi.getGroupName() + " idle thread") {
                                        @Override
                                        public void run() {
                                            try {
                                                Thread.sleep(1000 * gi.getTimeToIdelSeconds());
                                                synchronized (groupMap) {
                                                    if (gi.getJobs().isEmpty()) {
                                                        groupMap.remove(gi.getGroupName());
                                                    }
                                                }
                                            } catch (InterruptedException ex) {
                                                Logger.getLogger(Scheduler.class.getName()).log(Level.SEVERE, null, ex);
                                            }
                                        }
                                    };
                                    t.setDaemon(true);
                                    t.start();
                                }
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

        private final String groupName;
        private final int groupId;
        private final String user;
        private final Set<Integer> jobs = Collections.synchronizedNavigableSet(new TreeSet<Integer>());
        private int timeToIdelSeconds;

        private int priority;

        public GroupInfo(String groupName, String user, int timeToIdelSeconds) {
            this.groupName = groupName;
            this.groupId = groupCounter.incrementAndGet();
            this.user = user;
            this.timeToIdelSeconds = timeToIdelSeconds;
        }

        public String getGroupName() {
            return groupName;
        }

        public int getGroupId() {
            return groupId;
        }

        public String getUser() {
            return user;
        }

        public Set<Integer> getJobs() {
            return jobs;
        }

        public void setTimeToIdelSeconds(int timeToIdelSeconds) {
            this.timeToIdelSeconds = timeToIdelSeconds;
        }

        public int getPriority() {
            return priority;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public int getTimeToIdelSeconds() {
            return timeToIdelSeconds;
        }
    }

    public class ProcessInfo {

        private final int id;
        private final int pId;
        private final PeerChannel<SubmitInput> channel;
        private long maxSeenRSS;
        private int niceness = Integer.MAX_VALUE;

        public ProcessInfo(int id, int pId, PeerChannel<SubmitInput> channel) {
            this.id = id;
            this.pId = pId;
            this.channel = channel;
        }

        public PeerChannel<SubmitInput> getChannel() {
            return channel;
        }

        public int getPid() {
            return pId;
        }

        public long getMaxSeenRSS() {
            return maxSeenRSS;
        }

        public void setMaxSeenRSS(long maxSeenRSS) {
            this.maxSeenRSS = maxSeenRSS;
        }

        public int getNiceness() {
            return niceness;
        }

        public int getId() {
            return id;
        }

        public void setNiceness(int niceness) throws IOException, InterruptedException {
            if (niceness != this.niceness) {
                LinuxCommands.getInstance().setNiceness(pId, niceness);
                channel.sendEvent(Event.niceness, niceness);
                this.niceness = niceness;
            }
        }
    }
}
