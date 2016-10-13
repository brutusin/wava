package org.brutusin.wava.core;

import org.brutusin.wava.core.cfg.Config;
import org.brutusin.wava.core.plug.PromiseHandler;
import org.brutusin.wava.core.plug.LinuxCommands;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.brutusin.commons.utils.ErrorHandler;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.data.CancelInfo;
import org.brutusin.wava.data.PriorityInfo;
import org.brutusin.wava.data.SubmitInfo;
import org.brutusin.wava.data.Stats;
import org.brutusin.wava.utils.ANSIColor;
import org.brutusin.wava.utils.Utils;

public class Scheduler {

    private final static Logger LOGGER = Logger.getLogger(Scheduler.class.getName());
    private final static String DEFAULT_GROUP_NAME = "default";
    private final static int NICENESS_RANGE = Config.getInstance().getNicenessRange()[1] - Config.getInstance().getNicenessRange()[0];

    private final Map<Integer, PeerChannel<SubmitInfo>> jobMap = Collections.synchronizedMap(new HashMap<Integer, PeerChannel<SubmitInfo>>());
    private final Map<Integer, ProcessInfo> processMap = Collections.synchronizedMap(new HashMap<Integer, ProcessInfo>());
    private final Map<Integer, Integer> previousPositionMap = Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private final NavigableSet<Key> jobQueue = Collections.synchronizedNavigableSet(new TreeSet());
    private final Map<String, GroupInfo> groupMap = Collections.synchronizedMap(new HashMap());
    private final ThreadGroup threadGroup = new ThreadGroup(Scheduler.class.getName());
    private final AtomicInteger jobCounter = new AtomicInteger();
    private final AtomicInteger groupCounter = new AtomicInteger();
    private final Thread processingThread;
    private final Comparator<ProcessInfo> processComparator = new Comparator<ProcessInfo>() {
        @Override
        public int compare(ProcessInfo p1, ProcessInfo p2) {
            GroupInfo g1 = groupMap.get(p1.getChannel().getRequest().getGroupName());
            GroupInfo g2 = groupMap.get(p2.getChannel().getRequest().getGroupName());
            int ret = Integer.compare(g1.getPriority(), g2.getPriority());
            if (ret == 0) {
                ret = Integer.compare(p1.getId(), p2.getId());
            }
            return ret;
        }
    };

    private final String runningUser;
    private boolean closed;

    public Scheduler() throws IOException, InterruptedException {
        this.runningUser = LinuxCommands.getInstance().getRunningUser();
        createGroup(DEFAULT_GROUP_NAME, LinuxCommands.getInstance().getRunningUser(), 0, -1);
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

    private GroupInfo createGroup(String name, String user, int priority, int timetoIdleSeconds) {
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
        recomputeNiceness();
        long maxPromisedMemory = getMaxPromisedMemory();
        long availableMemory;
        if (Config.getInstance().getMaxTotalRSSBytes() > 0) {
            availableMemory = Config.getInstance().getMaxTotalRSSBytes() - maxPromisedMemory;
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
                    channel.sendEvent(Event.queued, position);
                    previousPositionMap.put(key.getId(), position);
                }
            }
        }
    }

    private void recomputeNiceness() throws IOException, InterruptedException {
        synchronized (processMap) {
            List<ProcessInfo> processes = new ArrayList<>(processMap.values());
            Collections.sort(processes, processComparator);
            for (int i = 0; i < processes.size(); i++) {
                ProcessInfo pi = processes.get(i);
                pi.setNiceness(Config.getInstance().getNicenessRange()[0] + (i * NICENESS_RANGE) / (processes.size() > 1 ? (processes.size() - 1) : 1));
            }
        }
    }

    private void checkPromises(long availableMemory) throws IOException, InterruptedException {
        int[] pIds = getPIds();
        if (pIds.length > 0) {
            Map<Integer, Stats> statMap = LinuxCommands.getInstance().getStats(pIds);
            if (statMap != null) {
                synchronized (processMap) {
                    for (Map.Entry<Integer, ProcessInfo> entry : processMap.entrySet()) {
                        ProcessInfo pi = entry.getValue();
                        Stats stats = statMap.get(pi.getPid());
                        if (stats != null) {
                            PeerChannel<SubmitInfo> channel = pi.getChannel();
                            if (stats.getRssBytes() > pi.getMaxSeenRSS()) {
                                pi.setMaxSeenRSS(stats.getRssBytes());
                            }
                            if (channel.getRequest().getMaxRSS() < stats.getRssBytes()) {
                                PromiseHandler.getInstance().promiseFailed(availableMemory, pi, stats);
                            }
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
            if (submitChannel.getRequest().getGroupName() == null) {
                submitChannel.getRequest().setGroupName(DEFAULT_GROUP_NAME);
            }
            int id = jobCounter.incrementAndGet();
            GroupInfo gi;
            synchronized (groupMap) {
                gi = groupMap.get(submitChannel.getRequest().getGroupName());
                if (gi == null) { // dynamic group
                    gi = createGroup(submitChannel.getRequest().getGroupName(), submitChannel.getUser(), 0, Config.getInstance().getDynamicGroupIdleSeconds());
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

    public void getRunningProcesses(PeerChannel<Void> channel) throws IOException, InterruptedException {
        try {
            StringBuilder header = new StringBuilder();
            header.append(ANSIColor.BLACK.getCode());
            header.append(ANSIColor.BG_GREEN.getCode());
            header.append(StringUtils.leftPad("PID", 8));
            header.append(" ");
            header.append(StringUtils.rightPad("GROUP", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("PRIORITY", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("JOB", 8));
            header.append(" ");
            header.append(StringUtils.rightPad("USER", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("NICE", 4));
            header.append(" ");
            header.append(StringUtils.leftPad("MAX_EXP_RSS", 12));
            header.append(" ");
            header.append(StringUtils.leftPad("MAX_SEEN_RSS", 12));
            header.append(" ");
            header.append("CMD");
            header.append(ANSIColor.END_OF_LINE.getCode());
            header.append(ANSIColor.RESET.getCode());
            PeerChannel.println(channel.getStdoutOs(), header.toString());
            synchronized (processMap) {
                for (ProcessInfo pi : processMap.values()) {
                    GroupInfo gi = groupMap.get(pi.getChannel().getRequest().getGroupName());
                    StringBuilder line = new StringBuilder();
                    line.append(StringUtils.leftPad(String.valueOf(pi.getPid()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(gi.getPriority()), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(pi.getId()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(pi.getChannel().getUser(), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(pi.getNiceness()), 4));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(pi.getChannel().getRequest().getMaxRSS()), 12));
                    line.append(" ");
                    if (pi.getMaxSeenRSS() > 0.9 * pi.getChannel().getRequest().getMaxRSS()) {
                        line.append(ANSIColor.RED.getCode());
                    }
                    line.append(StringUtils.leftPad(String.valueOf(pi.getMaxSeenRSS()), 12));
                    line.append(ANSIColor.RESET.getCode());
                    line.append(" ");
                    line.append(Arrays.toString(pi.getChannel().getRequest().getCommand()));
                    line.append(" ");
                    PeerChannel.println(channel.getStdoutOs(), line.toString());
                }
                if (processMap.size() == 0) {
                    channel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                } else {
                    channel.sendEvent(Event.retcode, 0);
                }
            }
        } finally {
            channel.close();
        }
    }

    public void cancel(PeerChannel<CancelInfo> cancelChannel) throws IOException, InterruptedException {
        try {
            synchronized (jobQueue) {
                if (closed) {
                    throw new IllegalStateException("Instance is closed");
                }
                PeerChannel<SubmitInfo> submitChannel = jobMap.get(cancelChannel.getRequest().getId());
                if (submitChannel != null) {
                    if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(submitChannel.getUser())) {
                        cancelChannel.log(ANSIColor.RED, "user '" + cancelChannel.getUser() + "' is not allowed to cancel a queued job from user '" + submitChannel.getUser() + "'");
                        cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                        return;
                    }
                    jobMap.remove(cancelChannel.getRequest().getId());
                    GroupInfo gi = groupMap.get(submitChannel.getRequest().getGroupName());
                    Key key = new Key(gi.getPriority(), gi.groupId, cancelChannel.getRequest().getId());
                    jobQueue.remove(key);
                    submitChannel.log(ANSIColor.YELLOW, "Cancelled by user '" + cancelChannel.getUser() + "'");
                    submitChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    submitChannel.close();
                    cancelChannel.log(ANSIColor.GREEN, "enqueued job sucessfully cancelled");
                    cancelChannel.sendEvent(Event.retcode, 0);
                    return;
                }
            }
            ProcessInfo pi = processMap.get(cancelChannel.getRequest().getId());
            if (pi != null) {
                if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(pi.getChannel().getUser())) {
                    cancelChannel.log(ANSIColor.RED, "user '" + cancelChannel.getUser() + "' is not allowed to terminate a running job from user '" + pi.getChannel().getUser() + "'");
                    cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    return;
                }
                LinuxCommands.getInstance().killTree(pi.getPid());
                cancelChannel.log(ANSIColor.GREEN, "running job sucessfully cancelled");
                cancelChannel.sendEvent(Event.retcode, 0);
            } else {
                cancelChannel.log(ANSIColor.RED, "job #" + cancelChannel.getRequest().getId() + " not found");
                cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
            }
        } finally {
            cancelChannel.close();
        }
    }

    public void setPriority(PeerChannel<PriorityInfo> channel) throws IOException {
        try {
            synchronized (jobQueue) {
                if (closed) {
                    throw new IllegalStateException("Instance is closed");
                }
                GroupInfo gi = groupMap.get(channel.getRequest().getGroupName());
                if (gi == null) {
                    channel.log(ANSIColor.RED, "unknown group name '" + channel.getRequest().getGroupName() + "'");
                    channel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    return;
                }
                int newPriority = channel.getRequest().getPriority();
                if (newPriority != gi.getPriority()) {
                    synchronized (gi.getJobs()) {
                        for (Integer id : gi.getJobs()) {
                            PeerChannel<SubmitInfo> submitChannel = jobMap.get(id);
                            if (submitChannel != null) {
                                Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                                jobQueue.remove(key);
                                Key newKey = new Key(newPriority, gi.getGroupId(), id);
                                jobQueue.add(newKey);
                            }
                            if (submitChannel == null) {
                                ProcessInfo pi = processMap.get(id);
                                if (pi != null) {
                                    submitChannel = pi.getChannel();
                                }
                            }
                            if (submitChannel != null) {
                                submitChannel.sendEvent(Event.priority, newPriority);
                            }
                        }
                    }
                    gi.setPriority(newPriority);
                }
                channel.sendEvent(Event.retcode, 0);
            }
        } finally {
            channel.close();
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
                        // starts runnning with more desfavorable niceness
                        pi.setNiceness(Config.getInstance().getNicenessRange()[1]);
                        processMap.put(id, pi);
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
                        processMap.remove(id);
                        final GroupInfo gi = groupMap.get(channel.getRequest().getGroupName());
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
        private final int timeToIdelSeconds;

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
        private final PeerChannel<SubmitInfo> channel;
        private long maxSeenRSS;
        private int niceness = Integer.MAX_VALUE;

        public ProcessInfo(int id, int pId, PeerChannel<SubmitInfo> channel) {
            this.id = id;
            this.pId = pId;
            this.channel = channel;
        }

        public PeerChannel<SubmitInfo> getChannel() {
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
