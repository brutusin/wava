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

    private final Map<Integer, JobInfo> jobMap = Collections.synchronizedMap(new HashMap<Integer, JobInfo>());
    private final Map<Integer, ProcessInfo> processMap = Collections.synchronizedMap(new HashMap<Integer, ProcessInfo>());
    private final Map<String, GroupInfo> groupMap = Collections.synchronizedMap(new HashMap<String, GroupInfo>());
    private final NavigableSet<Key> queueOrder = Collections.synchronizedNavigableSet(new TreeSet<Key>());
    private final NavigableSet<Key> runningOrder = Collections.synchronizedNavigableSet(new TreeSet<Key>());

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

    /**
     * Returns pIds of jobs in decreasing priority
     *
     * @return
     */
    private int[] getPIds() {
        synchronized (processMap) {
            int[] ret = new int[processMap.size()];
            int i = 0;
            synchronized (runningOrder) {
                for (Key key : runningOrder) {
                    ret[i++] = processMap.get(key.getId()).getPid();
                }
                return ret;
            }
        }
    }

    private long getMaxPromisedMemory() {
        synchronized (processMap) {
            long sum = 0;
            for (ProcessInfo pi : processMap.values()) {
                sum += pi.getJobInfo().getSubmitChannel().getRequest().getMaxRSS();
            }
            return sum;
        }
    }

    private void cleanStalePeers() throws InterruptedException {
        synchronized (queueOrder) {
            Iterator<Key> iterator = queueOrder.iterator();
            while (iterator.hasNext()) {
                Key key = iterator.next();
                JobInfo ji = jobMap.get(key.getId());
                if (!ji.getSubmitChannel().ping()) {
                    iterator.remove();
                    jobMap.remove(key.getId());
                }
            }
        }
        synchronized (runningOrder) {
            Iterator<Key> iterator = runningOrder.iterator();
            while (iterator.hasNext()) {
                Key key = iterator.next();
                JobInfo ji = jobMap.get(key.getId());
                ProcessInfo pi = processMap.get(key.getId());
                if (!ji.getSubmitChannel().ping()) {
                    iterator.remove();
                    jobMap.remove(key.getId());
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
        synchronized (queueOrder) {
            while (queueOrder.size() > 0) {
                final Key key = queueOrder.first();
                JobInfo ji = jobMap.get(key.getId());
                if (ji.getSubmitChannel().getRequest().getMaxRSS() > availableMemory) {
                    break;
                }
                queueOrder.pollFirst();
                execute(key.getId(), ji);
                availableMemory -= ji.getSubmitChannel().getRequest().getMaxRSS();
            }
            int position = 0;
            for (Key key : queueOrder) {
                position++;
                JobInfo ji = jobMap.get(key.getId());
                if (position != ji.getPreviousQueuePosition()) {
                    ji.getSubmitChannel().sendEvent(Event.queued, position);
                    ji.setPreviousQueuePosition(position);
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
                    synchronized (runningOrder) {
                        for (Key key : runningOrder) {
                            ProcessInfo pi = processMap.get(key.getId());
                            long treeRSS = treeRSSs[i++];
                            currentRSS += treeRSS;
                            if (treeRSS != 0) {
                                if (treeRSS > pi.getMaxSeenRSS()) {
                                    pi.setMaxSeenRSS(treeRSS);
                                }
                                if (pi.getMaxRSS() < treeRSS) {
                                    boolean allowed = PromiseHandler.getInstance().promiseFailed(availableMemory, pi, treeRSS);
                                    if (allowed) {
                                        availableMemory = availableMemory + pi.getJobInfo().getSubmitChannel().getRequest().getMaxRSS() - treeRSS;
                                        pi.setMaxRSS(treeRSS);
                                        pi.setAllowed(true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return currentRSS;
    }

    public void submit(PeerChannel<SubmitInput> submitChannel) throws IOException, InterruptedException {
        synchronized (queueOrder) {
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
            JobInfo ji = new JobInfo(id, submitChannel);
            jobMap.put(id, ji);
            submitChannel.sendEvent(Event.id, id);
            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
            queueOrder.add(key);
            submitChannel.sendEvent(Event.priority, gi.getPriority());
            refresh();
        }
    }

    public void listGroups(PeerChannel<Void> channel) throws IOException, InterruptedException {
        try {
            StringBuilder header = new StringBuilder(ANSICode.CLEAR.getCode());
            header.append(ANSICode.MOVE_TO_TOP.getCode());
            header.append(ANSICode.BLACK.getCode());
            header.append(ANSICode.BG_GREEN.getCode());
            header.append(StringUtils.rightPad("GROUP", 8));
            header.append(" ");
            header.append(StringUtils.rightPad("USER", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("PRIORITY", 8));
            header.append(" ");
            header.append(StringUtils.leftPad("IDLE_TIME", 9));
            header.append(" ");
            header.append(StringUtils.leftPad("JOBS", 5));
            header.append(ANSICode.END_OF_LINE.getCode());
            header.append(ANSICode.RESET.getCode());
            PeerChannel.println(channel.getStdoutOs(), header.toString());
            synchronized (groupMap) {
                for (GroupInfo gi : groupMap.values()) {
                    StringBuilder line = new StringBuilder();
                    line.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(gi.getUser(), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(gi.getPriority()), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(gi.getTimeToIdelSeconds()), 9));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(gi.getJobs().size()), 5));
                    PeerChannel.println(channel.getStdoutOs(), line.toString());
                }
            }
        } finally {
            channel.sendEvent(Event.retcode, 0);
            channel.close();
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
            synchronized (queueOrder) {
                synchronized (processMap) {
                    for (ProcessInfo pi : processMap.values()) {
                        GroupInfo gi = groupMap.get(pi.getJobInfo().getSubmitChannel().getRequest().getGroupName());
                        StringBuilder line = new StringBuilder();
                        line.append(StringUtils.leftPad(String.valueOf(pi.getJobInfo().getId()), 8));
                        line.append(" ");
                        line.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                        line.append(" ");
                        line.append(StringUtils.rightPad(pi.getJobInfo().getSubmitChannel().getUser(), 8));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(gi.getPriority()), 8));
                        line.append(" ");
                        line.append(StringUtils.leftPad("", 5));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(pi.getPid()), 8));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(pi.getNiceness()), 4));
                        line.append(" ");
                        line.append(StringUtils.leftPad(String.valueOf(pi.getJobInfo().getSubmitChannel().getRequest().getMaxRSS()), 12));
                        line.append(" ");
                        if (pi.getMaxSeenRSS() > 0.9 * pi.getJobInfo().getSubmitChannel().getRequest().getMaxRSS()) {
                            line.append(ANSICode.RED.getCode());
                        }
                        line.append(StringUtils.leftPad(String.valueOf(pi.getMaxSeenRSS()), 12));
                        line.append(ANSICode.RESET.getCode());
                        line.append(" ");
                        line.append(Arrays.toString(pi.getJobInfo().getSubmitChannel().getRequest().getCommand()));
                        line.append(" ");
                        PeerChannel.println(channel.getStdoutOs(), line.toString());
                    }
                }
                int position = 0;
                for (Key key : queueOrder) {
                    position++;
                    JobInfo ji = jobMap.get(key.getId());
                    StringBuilder line = new StringBuilder();
                    line.append(ANSICode.YELLOW.getCode());
                    line.append(StringUtils.leftPad(String.valueOf(key.getId()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(String.valueOf(ji.getSubmitChannel().getRequest().getGroupName()), 8));
                    line.append(" ");
                    line.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(key.getPriority()), 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(position), 5));
                    line.append(" ");
                    line.append(StringUtils.leftPad("", 8));
                    line.append(" ");
                    line.append(StringUtils.leftPad("", 4));
                    line.append(" ");
                    line.append(StringUtils.leftPad(String.valueOf(ji.getSubmitChannel().getRequest().getMaxRSS()), 12));
                    line.append(" ");
                    line.append(StringUtils.leftPad("", 12));
                    line.append(" ");
                    line.append(Arrays.toString(ji.getSubmitChannel().getRequest().getCommand()));
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
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            synchronized (queueOrder) {
                JobInfo ji = jobMap.get(cancelChannel.getRequest().getId());
                if (ji == null) {
                    cancelChannel.log(ANSICode.RED, "job not found");
                    cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    return;
                }
                if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(ji.getSubmitChannel().getUser())) {
                    cancelChannel.log(ANSICode.RED, "user '" + cancelChannel.getUser() + "' is not allowed to cancel a job from user '" + ji.getSubmitChannel().getUser() + "'");
                    cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    return;
                }
                jobMap.remove(cancelChannel.getRequest().getId());
                GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                Key key = new Key(gi.getPriority(), gi.groupId, cancelChannel.getRequest().getId());
                boolean queued = queueOrder.remove(key);
                if (queued) {
                    ji.getSubmitChannel().sendEvent(Event.retcode.cancelled, cancelChannel.getUser());
                    ji.getSubmitChannel().sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                    ji.getSubmitChannel().close();
                    cancelChannel.log(ANSICode.GREEN, "enqueued job sucessfully cancelled");
                    cancelChannel.sendEvent(Event.retcode, 0);
                    return;
                }
            }
            synchronized (processMap) {
                ProcessInfo pi = processMap.get(cancelChannel.getRequest().getId());
                if (pi != null) {
                    GroupInfo gi = groupMap.get(pi.getJobInfo().getSubmitChannel().getRequest().getGroupName());
                    Key key = new Key(gi.getPriority(), gi.groupId, cancelChannel.getRequest().getId());
                    runningOrder.remove(key);
                    LinuxCommands.getInstance().killTree(pi.getPid());
                    cancelChannel.log(ANSICode.GREEN, "running job sucessfully cancelled");
                    cancelChannel.sendEvent(Event.retcode, 0);
                } else {
                    cancelChannel.log(ANSICode.RED, "job #" + cancelChannel.getRequest().getId() + " not found");
                    cancelChannel.sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                }
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
                            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                            Key newKey = new Key(newPriority, gi.getGroupId(), id);
                            boolean enqueded;
                            synchronized (queueOrder) {
                                enqueded = queueOrder.remove(key);
                                if (enqueded) {
                                    queueOrder.add(newKey);
                                }
                            }
                            if (!enqueded) {
                                synchronized (processMap) {
                                    runningOrder.remove(key);
                                    runningOrder.add(newKey);
                                }
                            }
                            JobInfo ji = jobMap.get(id);
                            ji.getSubmitChannel().sendEvent(Event.priority, newPriority);
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

    private void execute(final int id, final JobInfo ji) {
        if (ji == null) {
            throw new IllegalArgumentException("Id is required");
        }
        Thread t;
        t = new Thread(this.threadGroup, "scheduled process " + id) {
            @Override
            public void run() {
                GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                String[] cmd = ji.getSubmitChannel().getRequest().getCommand();
                if (Scheduler.this.runningUser.equals("root")) {
                    cmd = LinuxCommands.getInstance().getRunAsCommand(ji.getSubmitChannel().getUser(), cmd);
                }
                cmd = LinuxCommands.getInstance().decorateWithCPUAffinity(cmd, Config.getInstance().getCpuAfinity());
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.environment().clear();
                pb.directory(ji.getSubmitChannel().getRequest().getWorkingDirectory());
                if (ji.getSubmitChannel().getRequest().getEnvironment() != null) {
                    pb.environment().putAll(ji.getSubmitChannel().getRequest().getEnvironment());
                }
                ProcessInfo pi;
                Process process;
                int pId;
                try {
                    try {
                        process = pb.start();
                        pId = Miscellaneous.getUnixId(process);
                        ji.getSubmitChannel().sendEvent(Event.running, pId);
                        pi = new ProcessInfo(ji, pId);
                        processMap.put(ji.getId(), pi);
                        synchronized (gi) {
                            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                            runningOrder.add(key);
                        }
                        updateNiceness(pId);
                    } catch (Exception ex) {
                        ji.getSubmitChannel().sendEvent(Event.error, JsonCodec.getInstance().transform(Miscellaneous.getStrackTrace(ex)));
                        ji.getSubmitChannel().sendEvent(Event.retcode, Utils.WAVA_ERROR_RETCODE);
                        return;
                    }
                    Thread stoutReaderThread = Miscellaneous.pipeAsynchronously(process.getInputStream(), (ErrorHandler) null, true, ji.getSubmitChannel().getStdoutOs());
                    stoutReaderThread.setName("stdout-pid-" + pId);
                    Thread sterrReaderThread = Miscellaneous.pipeAsynchronously(process.getErrorStream(), (ErrorHandler) null, true, ji.getSubmitChannel().getStderrOs());
                    sterrReaderThread.setName("stderr-pid-" + pId);
                    try {
                        int code = process.waitFor();
                        ji.getSubmitChannel().sendEvent(Event.maxrss, pi.getMaxSeenRSS());
                        ji.getSubmitChannel().sendEvent(Event.retcode, code);
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
                        ji.getSubmitChannel().close();
                        jobMap.remove(id);
                        processMap.remove(id);
                        synchronized (gi) {
                            Key key = new Key(gi.getPriority(), gi.getGroupId(), id);
                            runningOrder.remove(key);
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
        synchronized (queueOrder) {
            this.closed = true;
            this.threadGroup.interrupt();
        }
    }

    public class Key implements Comparable<Key> {

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

    public class GroupInfo {

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

    public class JobInfo {

        private final int id;
        private final PeerChannel<SubmitInput> submitChannel;

        private int previousQueuePosition;

        public JobInfo(int id, PeerChannel<SubmitInput> submitChannel) throws IOException, InterruptedException {
            this.id = id;
            this.submitChannel = submitChannel;
        }

        public int getPreviousQueuePosition() {
            return previousQueuePosition;
        }

        public void setPreviousQueuePosition(int previousQueuePosition) {
            this.previousQueuePosition = previousQueuePosition;
        }

        public int getId() {
            return id;
        }

        public PeerChannel<SubmitInput> getSubmitChannel() {
            return submitChannel;
        }
    }

    public class ProcessInfo {

        private final JobInfo jobInfo;
        private final int pId;
        private long maxRSS;
        private long maxSeenRSS;
        private int niceness = Integer.MAX_VALUE;
        private boolean allowed;

        public ProcessInfo(JobInfo jobInfo, int pId) {
            this.jobInfo = jobInfo;
            this.pId = pId;
            this.maxRSS = jobInfo.getSubmitChannel().getRequest().getMaxRSS();
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

        public JobInfo getJobInfo() {
            return jobInfo;
        }

        public int getpId() {
            return pId;
        }

        public long getMaxRSS() {
            return maxRSS;
        }

        public void setMaxRSS(long maxRSS) {
            this.maxRSS = maxRSS;
        }

        public boolean isAllowed() {
            return allowed;
        }

        public void setAllowed(boolean allowed) {
            this.allowed = allowed;
        }

        public void setNiceness(int niceness) throws IOException, InterruptedException {
            if (niceness != this.niceness) {
                LinuxCommands.getInstance().setNiceness(pId, niceness);
                jobInfo.getSubmitChannel().sendEvent(Event.niceness, niceness);
                this.niceness = niceness;
            }
        }
    }
}
