package org.brutusin.wava.core;

import org.brutusin.wava.io.Event;
import org.brutusin.wava.core.io.PeerChannel;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.core.plug.LinuxCommands;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.brutusin.commons.utils.ErrorHandler;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.cfg.GroupCfg;
import org.brutusin.wava.core.plug.LinuxCommands.TreeStats;
import org.brutusin.wava.core.plug.NicenessHandler;
import org.brutusin.wava.env.EnvEntry;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.ExtendedSubmitInput;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.utils.NonRootUserException;
import org.brutusin.wava.io.RetCode;

public class Scheduler {

    public final static String DEFAULT_GROUP_NAME = "default";
    public final static int EVICTION_ETERNAL = -1;

    private final static Logger LOGGER = Logger.getLogger(Scheduler.class.getName());

    // next four accessed under synchronized(jobSet)
    private final JobSet jobSet = new JobSet();
    private final Map<Integer, JobInfo> jobMap = new HashMap<>();
    private final Map<Integer, ProcessInfo> processMap = new HashMap<>();
    private final Map<String, GroupInfo> groupMap = new HashMap<>();

    private final ThreadGroup coreGroup = new ThreadGroup(Scheduler.class.getName());
    private final ThreadGroup processGroup = new ThreadGroup(Scheduler.class.getName() + " processes");

    private final AtomicInteger jobCounter = new AtomicInteger();
    private final AtomicInteger groupCounter = new AtomicInteger();
    private final Thread processingThread;

    private final String exitToken;

    private final long totalManagedRss;
    private final long maxJobRss;
    
    private final String runningUser;

    private volatile boolean closed;
    private volatile String jobList;

    private long lastPingMillis;
    private long refreshMillis;

    public Scheduler() throws NonRootUserException {
        this.runningUser = LinuxCommands.getInstance().getRunningUser();
        if (!this.runningUser.equals("root")) {
            throw new NonRootUserException();
        }
        this.totalManagedRss = Miscellaneous.parseHumanReadableByteCount(Config.getInstance().getSchedulerCfg().getSchedulerCapacity());
        this.maxJobRss = Miscellaneous.parseHumanReadableByteCount(Config.getInstance().getSchedulerCfg().getMaxJobSize());
        boolean cgroupCreated = LinuxCommands.getInstance().createWavaMemoryCgroup(totalManagedRss);
        if (!cgroupCreated) {
            throw new RuntimeException("Unable to create base cgroup folder");
        }
        createGroupInfo(DEFAULT_GROUP_NAME, this.runningUser, 0, EVICTION_ETERNAL);
        GroupCfg.Group[] predefinedGroups = Config.getInstance().getGroupCfg().getPredefinedGroups();
        if (predefinedGroups != null) {
            for (GroupCfg.Group group : predefinedGroups) {
                createGroupInfo(group.getName(), this.runningUser, group.getPriority(), group.getTimeToIdleSeconds());
            }
        }

        this.exitToken = RandomStringUtils.randomAlphabetic(10);
        this.jobList = createJobList(false, getAvailableManagedMemory(0), 0, 0);

        this.processingThread = new Thread(this.coreGroup, "processingThread") {
            @Override
            public void run() {
                while (true) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    try {
                        Thread.sleep(Config.getInstance().getSchedulerCfg().getRefreshLoopSleepMillisecs());
                        refresh();
                    } catch (Throwable th) {
                        if (th instanceof InterruptedException) {
                            break;
                        }
                        LOGGER.log(Level.SEVERE, th.getMessage(), th);
                    }
                }
            }
        };
    }

    public void start() {
        if (closed) {
            throw new IllegalStateException("Instance is closed");
        }
        this.processingThread.start();
    }

    private GroupInfo createGroupInfo(String name, String user, Integer priority, Integer timetoIdleSeconds) {
        synchronized (jobSet) {
            if (!groupMap.containsKey(name)) {
                if (priority == null) {
                    priority = 0;
                }
                if (timetoIdleSeconds == null) {
                    timetoIdleSeconds = -1;
                }
                GroupInfo gi = new GroupInfo(name, user, timetoIdleSeconds);
                gi.setPriority(priority);
                groupMap.put(gi.getGroupName(), gi);
                return gi;
            }
        }
        return null;
    }

    private void killForStarvationProtection(ProcessInfo pi) {
        try {
            if (pi.getJobInfo().getSubmitChannel().getRequest().isIdempotent()) {
                LOGGER.log(Level.WARNING, "Starvation scenario found. Ralaunching idempotent job {0} ({1})", new Object[]{pi.getJobInfo().getId(), pi.getJobInfo().getSubmitChannel().getRequest().getGroupName()});
                pi.getJobInfo().getSubmitChannel().sendEvent(Event.starvation_relaunch, runningUser);
                pi.getJobInfo().setRelaunched(true);
            } else {
                LOGGER.log(Level.SEVERE, "Starvation scenario found. Killing non-idempotent job {0} ({1})", new Object[]{pi.getJobInfo().getId(), pi.getJobInfo().getSubmitChannel().getRequest().getGroupName()});
                pi.getJobInfo().getSubmitChannel().sendEvent(Event.starvation_stop, runningUser);
            }
            LinuxCommands.getInstance().killTree(pi.getPid());
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    /**
     * Returns pIds of jobs in decreasing priority
     *
     * @return
     */
    private int[] getPIds() {
        synchronized (jobSet) {
            int[] ret = new int[jobSet.countRunning()];
            Iterator<Integer> running = jobSet.getRunning();
            int i = 0;
            while (running.hasNext()) {
                Integer id = running.next();
                ProcessInfo pi = processMap.get(id);
                if (pi != null) {
                    ret[i++] = pi.getPid();
                } else {
                    ret[i++] = -1;
                }
            }
            return ret;
        }
    }

    private long getAllocatedManagedMemory() {
        synchronized (jobSet) {
            long sum = 0;
            JobSet.RunningIterator it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                sum += ji.getSubmitChannel().getRequest().getMaxRSS();
            }
            return sum;
        }
    }

    private void cleanStalePeers() throws InterruptedException {
        if (System.currentTimeMillis() - lastPingMillis < Config.getInstance().getSchedulerCfg().getPingMillisecs()) {
            return;
        }
        synchronized (jobSet) {
            Iterator<Integer> it = jobSet.getQueue();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                if (!ji.getSubmitChannel().ping()) {
                    it.remove();
                    removeFromJobMap(ji);
                    GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                    gi.getJobs().remove(id);
                    try {
                        ji.getSubmitChannel().close();
                    } catch (IOException ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                }
            }

            it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                ProcessInfo pi = processMap.get(id);
                if (pi != null && !pi.getJobInfo().getSubmitChannel().ping()) {
                    try {
                        LinuxCommands.getInstance().killTree(pi.getPid());
                    } catch (RuntimeException ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                }
            }
        }
        lastPingMillis = System.currentTimeMillis();
    }

    private void sendQueuePositionEventsToParentJobs() {
        synchronized (jobSet) {
            int position = 0;
            JobSet.QueueIterator it = jobSet.getQueue();
            while (it.hasNext()) {
                position++;
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                if (ji.getSubmitChannel().getRequest().getParentId() == null && position != ji.getPreviousQueuePosition()) {
                    ji.getSubmitChannel().sendEvent(Event.queued, position);
                    ji.setPreviousQueuePosition(position);
                }
            }
        }
    }

    private int getGroupsRunning() {
        int ret = 0;
        synchronized (jobSet) {
            ProcessInfo prev = null;
            Iterator<Integer> it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                ProcessInfo pi = processMap.get(id);
                if (pi != null) {
                    if (prev == null || !prev.getJobInfo().getSubmitChannel().getRequest().getGroupName().equals(pi.getJobInfo().getSubmitChannel().getRequest().getGroupName())) {
                        ret++;
                    }
                }
                prev = pi;
            }
        }
        return ret;

    }

    private void distributeNiceness() throws IOException, InterruptedException {
        synchronized (jobSet) {
            int groupsRunning = getGroupsRunning();
            int pos = 0;
            int gpos = 0;
            ProcessInfo prev = null;
            Iterator<Integer> it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                ProcessInfo pi = processMap.get(id);
                if (pi != null) {
                    if (prev != null && !prev.getJobInfo().getSubmitChannel().getRequest().getGroupName().equals(pi.getJobInfo().getSubmitChannel().getRequest().getGroupName())) {
                        gpos++;
                        if (groupsRunning <= gpos) {
                            groupsRunning = gpos + 1;
                        }
                    }
                    pi.setNiceness(NicenessHandler.getInstance().getNiceness(pos, jobSet.countRunning(), gpos, groupsRunning, Config.getInstance().getProcessCfg().getNicenessRange()[0], Config.getInstance().getProcessCfg().getNicenessRange()[1]));
                }
                pos++;
                prev = pi;
            }
        }
    }

    private void checkStarvation() {

        synchronized (jobSet) {
            if (jobSet.countQueued() == 0) {
                return;
            }
            boolean allJobsBlocked = true;
            ProcessInfo candidateToKill = null;
            long maxRSsSumOfBlockedJobs = 0;

            Iterator<Integer> it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                if (ji.getQueuedChildCount() > 0 && ji.getRunningChildCount() == 0) {
                    maxRSsSumOfBlockedJobs += ji.getSubmitChannel().getRequest().getMaxRSS();
                    ProcessInfo pi = processMap.get(id);
                    if (pi != null) {
                        if (pi.getJobInfo().isRelaunched()) {
                            maxRSsSumOfBlockedJobs -= ji.getSubmitChannel().getRequest().getMaxRSS();
                            continue;
                        }
                        if (candidateToKill == null || !candidateToKill.getJobInfo().getSubmitChannel().getRequest().isIdempotent() || ji.getSubmitChannel().getRequest().isIdempotent()) {
                            candidateToKill = pi;
                        }
                    } else {
                        candidateToKill = null;
                    }
                } else {
                    allJobsBlocked = false;
                }
            }
            if (candidateToKill != null) {
                if (maxRSsSumOfBlockedJobs > totalManagedRss * Config.getInstance().getSchedulerCfg().getMaxBlockedRssStarvationRatio()) {
                    killForStarvationProtection(candidateToKill);
                } else if (allJobsBlocked) {
                    JobInfo firstQueued = jobMap.get(jobSet.getQueue().next());
                    if (maxRSsSumOfBlockedJobs + firstQueued.getSubmitChannel().getRequest().getMaxRSS() > this.totalManagedRss) {
                        killForStarvationProtection(candidateToKill);
                    }
                }
            }
        }
    }

    private void dequeueJobs(long availableMemory) {
        synchronized (jobSet) {
            JobSet.QueueIterator queuedIt = jobSet.getQueue();
            while (queuedIt.hasNext()) {
                Integer id = queuedIt.next();
                JobInfo ji = jobMap.get(id);
                if (ji.getSubmitChannel().getRequest().getMaxRSS() > availableMemory) {
                    return;
                }
                queuedIt.moveToRunning();
                changeQueuedChildren(ji.getSubmitChannel().getRequest().getParentId(), false);
                changeRunningChildren(ji.getSubmitChannel().getRequest().getParentId(), true);
                execute(id, ji);
                availableMemory -= ji.getSubmitChannel().getRequest().getMaxRSS();
            }
        }
    }

    private long getAvailableManagedMemory(long allocatedManagedMemory) {
        long availableManagedMemory = totalManagedRss - allocatedManagedMemory;
        long systemAvailableMemory = LinuxCommands.getInstance().getMemInfo()[1];
        if (availableManagedMemory > systemAvailableMemory) {
            availableManagedMemory = systemAvailableMemory;
        }
        return availableManagedMemory;
    }

    private long getCurrentlyUsedManagedMemory() {
        int[] pIds = getPIds();
        TreeStats[] treeStats = LinuxCommands.getInstance().getTreeStats(pIds);
        long ret = 0;
        for (int i = 0; i < treeStats.length; i++) {
            ret += treeStats[i].rssBytes;
        }
        return ret;
    }

    private void refresh() throws IOException, InterruptedException {
        synchronized (jobSet) {
            if (closed) {
                return;
            }
            long start = System.currentTimeMillis();
            cleanStalePeers();
            distributeNiceness();
            long allocatedManagedMemory = getAllocatedManagedMemory();
            long availableManagedMemory = getAvailableManagedMemory(allocatedManagedMemory);
            long currentlyUsedManagedMemory = getStats();
            dequeueJobs(availableManagedMemory);
            sendQueuePositionEventsToParentJobs();
            this.jobList = createJobList(false, availableManagedMemory, allocatedManagedMemory, currentlyUsedManagedMemory);
            checkStarvation();
            this.refreshMillis = System.currentTimeMillis() - start;
        }
    }

    /**
     *
     * @param pi
     * @return ret[0]: job position, ret[1]: group position
     * @throws IOException
     * @throws InterruptedException
     */
    private int[] getRunningPosition(ProcessInfo pi) throws IOException, InterruptedException {
        synchronized (jobSet) {
            JobSet.RunningIterator running = jobSet.getRunning();
            int pos = 0;
            int gpos = 0;
            ProcessInfo prev = null;
            while (running.hasNext()) {
                Integer id = running.next();
                ProcessInfo p = processMap.get(id);
                if (p == null) {
                    continue;
                }
                if (prev != null && !prev.getJobInfo().getSubmitChannel().getRequest().getGroupName().equals(p.getJobInfo().getSubmitChannel().getRequest().getGroupName())) {
                    gpos++;
                }
                if (pi.getJobInfo().getId() == p.getJobInfo().getId()) {
                    return new int[]{pos, gpos};
                }
                pos++;
                prev = p;
            }
            return null;
        }
    }

    private long getStats() throws IOException, InterruptedException {
        synchronized (jobSet) {
            long totalCurrentRss = 0;
            int[] pIds = getPIds();
            if (pIds.length > 0) {
                TreeStats[] treeStats = LinuxCommands.getInstance().getTreeStats(pIds);
                if (treeStats != null) {
                    JobSet.RunningIterator running = jobSet.getRunning();
                    int i = 0;
                    while (running.hasNext()) {
                        Integer id = running.next();
                        ProcessInfo pi = processMap.get(id);
                        if (pi == null) {
                            continue;
                        }
                        LinuxCommands.CgroupMemoryStats cgroupMemoryStats = LinuxCommands.getInstance().getCgroupMemoryStats(id);
                        if (cgroupMemoryStats != null) {
                            pi.setRss(cgroupMemoryStats.rssBytes);
                            if (pi.getRss() > pi.getMaxRss()) {
                                pi.setMaxRss(pi.getRss());
                            }
                            pi.setSwap(cgroupMemoryStats.swapBytes);
                            if (pi.getSwap() > pi.getMaxSwap()) {
                                pi.setMaxSwap(pi.getSwap());
                            }
                            totalCurrentRss += cgroupMemoryStats.rssBytes;
                        }
                        TreeStats st = treeStats[i++];
                        if (st != null) {
                            pi.setCpuUsage(st.cpuPercentage);
                        }
                    }
                }
            }
            return totalCurrentRss;
        }
    }

    public void submit(PeerChannel<ExtendedSubmitInput> submitChannel) throws IOException, InterruptedException {

        if (closed) {
            submitChannel.sendEvent(Event.retcode, RetCode.CANCELLED.getCode());
            submitChannel.close();
            return;
        }

        if (submitChannel == null) {
            throw new IllegalArgumentException("Request info is required");
        }

        if (maxJobRss > 0 && submitChannel.getRequest().getMaxRSS() > maxJobRss) {
            submitChannel.getRequest().setMaxRSS(maxJobRss);
        }
        if (totalManagedRss < submitChannel.getRequest().getMaxRSS()) {
            submitChannel.getRequest().setMaxRSS(totalManagedRss);
        }

        long treeRSS = submitChannel.getRequest().getMaxRSS();
        Integer parentId = submitChannel.getRequest().getParentId();

        while (parentId != null) {
            JobInfo ji = jobMap.get(parentId);
            if (ji == null) {
                break;
            }
            treeRSS += ji.getSubmitChannel().getRequest().getMaxRSS();
            parentId = ji.getSubmitChannel().getRequest().getParentId();
        }

        if (treeRSS > totalManagedRss) {
            submitChannel.sendEvent(Event.exceed_tree, treeRSS);
            submitChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
            submitChannel.close();
            return;
        }

        if (submitChannel.getRequest().getGroupName() == null) {
            submitChannel.getRequest().setGroupName(DEFAULT_GROUP_NAME);
        }

        synchronized (jobSet) {
            JobInfo ji = new JobInfo(jobCounter.incrementAndGet(), submitChannel);
            GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
            if (gi == null) { // dynamic group
                gi = createGroupInfo(ji.getSubmitChannel().getRequest().getGroupName(), ji.getSubmitChannel().getUser(), 0, Config.getInstance().getGroupCfg().getDynamicGroupIdleSeconds());
            }
            gi.getJobs().add(ji.getId());
            changeQueuedChildren(ji.getSubmitChannel().getRequest().getParentId(), true);
            jobMap.put(ji.getId(), ji);
            ji.getSubmitChannel().sendEvent(Event.id, ji.getId());
            jobSet.queue(ji.getId(), gi.getPriority(), gi.getGroupId());
            ji.getSubmitChannel().sendEvent(Event.priority, gi.getPriority());
        }
    }

    private String createJobList(boolean noHeaders, long availableManagedMemory, long allocatedManagedMemory, long currentlyUsedManagedMemory) {
        StringBuilder sb = new StringBuilder(200);
        try {
            if (!noHeaders) {
                sb.append(ANSICode.BLACK.getCode());
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(ANSICode.BOLD.getCode());
                sb.append(StringUtils.center("JOB INFO", 46));
                sb.append(ANSICode.BG_BLACK.getCode());
                sb.append(" ");
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(StringUtils.center("PROCESS STATS", 64));
                sb.append(ANSICode.BG_BLACK.getCode());
                sb.append(" ");
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(" COMMAND");
                sb.append(ANSICode.END_OF_LINE.getCode());
                sb.append(ANSICode.RESET.getCode());
                sb.append(ANSICode.BLACK.getCode());
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append("\n");
                sb.append(StringUtils.leftPad("JOB ID", 8));
                sb.append(" ");
                sb.append(StringUtils.leftPad("PARENT", 8));
                sb.append(" ");
                sb.append(StringUtils.rightPad("GROUP", 8));
                sb.append(" ");
                sb.append(StringUtils.rightPad("USER", 8));
                sb.append(" ");
                sb.append(StringUtils.leftPad("JOB_RSS", 10));
                sb.append(ANSICode.BG_BLACK.getCode());
                sb.append(" ");
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(StringUtils.leftPad("PID", 8));
                sb.append(" ");
                sb.append(StringUtils.leftPad("NICE", 4));
                sb.append(" ");
                sb.append(StringUtils.leftPad("MAX_RSS", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("RSS  ", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("MAX_SWAP", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("SWAP  ", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("CPU%", 6));
                sb.append(ANSICode.BG_BLACK.getCode());
                sb.append(" ");
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(ANSICode.END_OF_LINE.getCode());
                sb.append(ANSICode.RESET.getCode());
            } else {
                ANSICode.setActive(false);
            }
            int blocked = 0;
            synchronized (jobSet) {
                JobSet.RunningIterator runningIterator = jobSet.getRunning();
                while (runningIterator.hasNext()) {
                    Integer id = runningIterator.next();
                    JobInfo ji = jobMap.get(id);
                    ProcessInfo pi = processMap.get(id);
                    GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                    sb.append("\n");
                    sb.append(ANSICode.NO_WRAP.getCode());
                    if (pi != null) {
                        if (ji.getRunningChildCount() == 0 && ji.getQueuedChildCount() > 0) {
                            sb.append(ANSICode.RED.getCode());
                            blocked++;
                        } else {
                            sb.append(ANSICode.GREEN.getCode());
                        }
                        sb.append(StringUtils.leftPad(String.valueOf(id), 8));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");
                        String pId;
                        if (ji.getSubmitChannel().getRequest().getParentId() != null) {
                            pId = String.valueOf(ji.getSubmitChannel().getRequest().getParentId());
                        } else {
                            pId = "";
                        }
                        sb.append(StringUtils.leftPad(pId, 8));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                        sb.append(" ");
                        String[] mem = Miscellaneous.humanReadableByteCount(ji.getSubmitChannel().getRequest().getMaxRSS(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad(String.valueOf(pi.getPid()), 8));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad(String.valueOf(pi.getNiceness()), 4));
                        sb.append(" ");
                        mem = Miscellaneous.humanReadableByteCount(pi.getMaxRss(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");
                        mem = Miscellaneous.humanReadableByteCount(pi.getRss(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");
                        if (pi.getMaxSwap() > ji.getSubmitChannel().getRequest().getMaxRSS() / 2) {
                            sb.append(ANSICode.RED.getCode());
                        } else if (pi.getMaxSwap() > 0) {
                            sb.append(ANSICode.YELLOW.getCode());
                        }
                        mem = Miscellaneous.humanReadableByteCount(pi.getMaxSwap(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");
                        if (pi.getSwap() > ji.getSubmitChannel().getRequest().getMaxRSS() / 2) {
                            sb.append(ANSICode.RED.getCode());
                        } else if (pi.getSwap() > 0) {
                            sb.append(ANSICode.YELLOW.getCode());
                        }
                        mem = Miscellaneous.humanReadableByteCount(pi.getSwap(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");
                        sb.append(StringUtils.leftPad(String.format("%.1f", pi.getCpuUsage()), 6));
                        sb.append(" ");
                        sb.append(Arrays.toString(ji.getSubmitChannel().getRequest().getCommand()));
                        sb.append(" ");
                    } else { // process not stated yet
                        sb.append(StringUtils.leftPad(String.valueOf(id), 8));
                        sb.append(" ");
                        String pId;
                        if (ji.getSubmitChannel().getRequest().getParentId() != null) {
                            pId = String.valueOf(ji.getSubmitChannel().getRequest().getParentId());
                        } else {
                            pId = "";
                        }
                        sb.append(StringUtils.leftPad(pId, 8));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                        sb.append(" ");
                        String[] mem = Miscellaneous.humanReadableByteCount(ji.getSubmitChannel().getRequest().getMaxRSS(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 8));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 4));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 6));
                        sb.append(" ");
                        sb.append(Arrays.toString(ji.getSubmitChannel().getRequest().getCommand()));
                        sb.append(" ");
                    }
                    sb.append(ANSICode.WRAP.getCode());
                }
                JobSet.QueueIterator queueIterator = jobSet.getQueue();
                while (queueIterator.hasNext()) {
                    Integer id = queueIterator.next();
                    JobInfo ji = jobMap.get(id);
                    sb.append("\n");
                    sb.append(ANSICode.NO_WRAP.getCode());
                    sb.append(ANSICode.YELLOW.getCode());
                    sb.append(StringUtils.leftPad(String.valueOf(id), 8));
                    sb.append(" ");
                    String pId;
                    if (ji.getSubmitChannel().getRequest().getParentId() != null) {
                        pId = String.valueOf(ji.getSubmitChannel().getRequest().getParentId());
                    } else {
                        pId = "";
                    }
                    sb.append(StringUtils.leftPad(pId, 8));
                    sb.append(" ");
                    sb.append(StringUtils.rightPad(String.valueOf(ji.getSubmitChannel().getRequest().getGroupName()), 8));
                    sb.append(" ");
                    sb.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                    sb.append(" ");
                    String[] mem = Miscellaneous.humanReadableByteCount(ji.getSubmitChannel().getRequest().getMaxRSS(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                    sb.append(StringUtils.leftPad(mem[0], 6));
                    sb.append(" ");
                    sb.append(StringUtils.rightPad(mem[1], 3));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 8));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 4));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 6));
                    sb.append(" ");
                    sb.append(Arrays.toString(ji.getSubmitChannel().getRequest().getCommand()));
                    sb.append(" ");
                    sb.append(ANSICode.RESET.getCode());
                    sb.append(ANSICode.WRAP.getCode());
                }
            }
            if (!noHeaders) {
                StringBuilder statSb = new StringBuilder();
                statSb.append(ANSICode.CLEAR.getCode());
                statSb.append(ANSICode.MOVE_TO_TOP.getCode());
                statSb.append("\n");
                statSb.append(ANSICode.CYAN);
                statSb.append("  Jobs: ");
                statSb.append(jobSet.countQueued() + jobSet.countRunning());
                statSb.append("; ");
                statSb.append(ANSICode.GREEN);
                statSb.append(jobSet.countRunning() - blocked);
                statSb.append(ANSICode.CYAN);
                statSb.append(" running; ");
                statSb.append(ANSICode.RED);
                statSb.append(blocked);
                statSb.append(ANSICode.CYAN);
                statSb.append(" bloqued; ");
                statSb.append(ANSICode.YELLOW);
                statSb.append(jobSet.countQueued());
                statSb.append(ANSICode.CYAN);
                statSb.append(" queued");
                statSb.append("\n");
                statSb.append("  Refresh time: ");
                statSb.append(ANSICode.GREEN);
                statSb.append(this.refreshMillis);
                statSb.append(ANSICode.CYAN);
                statSb.append(" ms");
                statSb.append("\n");
                statSb.append("  Memory: managed ");
                statSb.append(Miscellaneous.humanReadableByteCount(totalManagedRss, Config.getInstance().getuICfg().issIMemoryUnits()));
                statSb.append(ANSICode.CYAN);
                statSb.append(" (");
                statSb.append(ANSICode.GREEN);
                statSb.append(Miscellaneous.humanReadableByteCount(availableManagedMemory, Config.getInstance().getuICfg().issIMemoryUnits()));
                statSb.append(ANSICode.CYAN);
                statSb.append(" / ");
                statSb.append(ANSICode.YELLOW);
                statSb.append(Miscellaneous.humanReadableByteCount(allocatedManagedMemory, Config.getInstance().getuICfg().issIMemoryUnits()));
                statSb.append(ANSICode.CYAN);
                statSb.append("); using ");
                statSb.append(ANSICode.RED);
                statSb.append(Miscellaneous.humanReadableByteCount(currentlyUsedManagedMemory, Config.getInstance().getuICfg().issIMemoryUnits()));
                statSb.append(ANSICode.RESET);
                statSb.append("\n");
                statSb.append("\n");
                sb.insert(0, statSb);
            }
        } finally {
            ANSICode.setActive(true);
        }
        return sb.toString();
    }

    public void listGroups(PeerChannel<Void> channel, boolean noHeaders) throws IOException, InterruptedException {
        try {
            if (!noHeaders) {
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
            } else {
                ANSICode.setActive(false);
            }
            synchronized (jobSet) {
                TreeSet<GroupInfo> groups = new TreeSet<>(groupMap.values());
                for (GroupInfo gi : groups) {
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
            ANSICode.setActive(true);
            channel.sendEvent(Event.retcode, 0);
            channel.close();
        }
    }

    public void listJobs(PeerChannel<Void> channel, boolean noHeaders) throws IOException, InterruptedException {
        try {
            if (noHeaders) {
                long allocatedManagedMemory = getAllocatedManagedMemory();
                long availableManagedMemory = getAvailableManagedMemory(allocatedManagedMemory);
                PeerChannel.println(channel.getStdoutOs(), createJobList(true, availableManagedMemory, allocatedManagedMemory, getCurrentlyUsedManagedMemory()));
            } else if (!closed) {
                PeerChannel.println(channel.getStdoutOs(), jobList);
            } else {
                long allocatedManagedMemory = getAllocatedManagedMemory();
                long availableManagedMemory = getAvailableManagedMemory(allocatedManagedMemory);
                PeerChannel.println(channel.getStdoutOs(), createJobList(false, availableManagedMemory, allocatedManagedMemory, getCurrentlyUsedManagedMemory()));
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
            int id = cancelChannel.getRequest().getId();
            synchronized (jobSet) {
                JobSet.State state = jobSet.getState(id);
                if (state == null) {
                    cancelChannel.log(ANSICode.RED, "job not found");
                    cancelChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                } else if (state == JobSet.State.queued) {
                    JobInfo ji = jobMap.get(id);
                    if (ji != null) {
                        if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(ji.getSubmitChannel().getUser())) {
                            cancelChannel.log(ANSICode.RED, "user '" + cancelChannel.getUser() + "' is not allowed to cancel a job from user '" + ji.getSubmitChannel().getUser() + "'");
                            cancelChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                            return;
                        }
                        ji.getSubmitChannel().sendEvent(Event.cancelled, cancelChannel.getUser());
                        ji.getSubmitChannel().sendEvent(Event.retcode, RetCode.CANCELLED.getCode());
                        ji.getSubmitChannel().close();
                        cancelChannel.log(ANSICode.GREEN, "enqueued job sucessfully cancelled");
                        cancelChannel.sendEvent(Event.retcode, 0);
                        GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                        gi.getJobs().remove(id);
                        jobSet.remove(id);
                        removeFromJobMap(ji);
                    } else {
                        throw new AssertionError();
                    }
                } else if (state == JobSet.State.running) {
                    ProcessInfo pi = processMap.get(id);
                    if (pi != null) {
                        if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(pi.getJobInfo().getSubmitChannel().getUser())) {
                            cancelChannel.log(ANSICode.RED, "user '" + cancelChannel.getUser() + "' is not allowed to cancel a job from user '" + pi.getJobInfo().getSubmitChannel().getUser() + "'");
                            cancelChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                            return;
                        }
                        pi.getJobInfo().getSubmitChannel().sendEvent(Event.cancelled, cancelChannel.getUser());
                        LinuxCommands.getInstance().killTree(pi.getPid());
                        cancelChannel.log(ANSICode.GREEN, "running job sucessfully cancelled");
                        cancelChannel.sendEvent(Event.retcode, 0);
                    }
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
            synchronized (jobSet) {
                GroupInfo gi = groupMap.get(channel.getRequest().getGroupName());
                if (gi == null) {
                    createGroupInfo(channel.getRequest().getGroupName(), channel.getUser(), channel.getRequest().getPriority(), channel.getRequest().getTimetoIdleSeconds());
                    channel.log(ANSICode.GREEN, "Group '" + channel.getRequest().getGroupName() + "' created successfully");
                    channel.sendEvent(Event.retcode, 0);
                    return;
                } else if (channel.getRequest().isDelete()) {
                    if (!channel.getUser().equals("root") && !channel.getUser().equals(gi.getUser())) {
                        if (gi.getUser().equals("root")) {
                            channel.log(ANSICode.RED, "Group '" + channel.getRequest().getGroupName() + "' can only be updated by user 'root'");
                        } else {
                            channel.log(ANSICode.RED, "Group '" + channel.getRequest().getGroupName() + "' can only be updated by users 'root' and '" + gi.getUser() + "'");
                        }
                        channel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                        return;
                    }
                    if (gi.getJobs().isEmpty()) {
                        channel.log(ANSICode.GREEN, "Group '" + channel.getRequest().getGroupName() + "' deleted successfully");
                        groupMap.remove(channel.getRequest().getGroupName());
                        channel.sendEvent(Event.retcode, 0);
                        return;
                    } else {
                        channel.log(ANSICode.RED, "Group '" + channel.getRequest().getGroupName() + "' cannot be deleted, since it contains " + gi.getJobs().size() + " active jobs");
                        channel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                        return;
                    }
                }
                Integer newPriority = channel.getRequest().getPriority();
                if (newPriority != null && newPriority != gi.getPriority()) {
                    synchronized (gi.getJobs()) {
                        for (Integer id : gi.getJobs()) {
                            jobSet.setPriority(id, newPriority, gi.getGroupId());
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

    private void removeFromJobMap(JobInfo jobInfo) {
        jobMap.remove(jobInfo.getId());
        JobSet.State state = jobSet.getState(jobInfo.getId());
        if (state == JobSet.State.queued) {
            changeQueuedChildren(jobInfo.getSubmitChannel().getRequest().getParentId(), false);
        } else {
            changeRunningChildren(jobInfo.getSubmitChannel().getRequest().getParentId(), false);
        }
    }

    private void changeQueuedChildren(Integer parentId, boolean increase) {
        if (parentId != null) {
            JobInfo parent = jobMap.get(parentId);
            if (parent != null) {
                synchronized (parent) {
                    parent.setQueuedChildCount(parent.getQueuedChildCount() + (increase ? 1 : -1));
                }
            }
        }
    }

    private void changeRunningChildren(Integer parentId, boolean increase) {
        if (parentId != null) {
            JobInfo parent = jobMap.get(parentId);
            if (parent != null) {
                synchronized (parent) {
                    parent.setRunningChildCount(parent.getRunningChildCount() + (increase ? 1 : -1));
                }
            }
        }
    }

    private void execute(final int id, final JobInfo ji) {
        if (ji == null) {
            throw new IllegalArgumentException("Id is required");
        }
        LinuxCommands.getInstance().createJobMemoryCgroup(id, ji.getSubmitChannel().getRequest().getMaxRSS());
        Thread t = new Thread(this.processGroup, "scheduled process " + id) {
            @Override
            public void run() {
                String[] cmd = ji.getSubmitChannel().getRequest().getCommand();
                cmd = LinuxCommands.getInstance().decorateRunAsCommand(cmd, ji.getSubmitChannel().getUser());
                cmd = LinuxCommands.getInstance().decorateWithCPUAffinity(cmd, Config.getInstance().getProcessCfg().getCpuAfinity());
                cmd = LinuxCommands.getInstance().decorateWithBatchSchedulerPolicy(cmd);
                cmd = LinuxCommands.getInstance().decorateRunInCgroup(cmd, id);
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.environment().clear();
                pb.directory(ji.getSubmitChannel().getRequest().getWorkingDirectory());
                if (ji.getSubmitChannel().getRequest().getEnvironment() != null) {
                    pb.environment().putAll(ji.getSubmitChannel().getRequest().getEnvironment());
                }
                pb.environment().put(EnvEntry.WAVA_JOB_ID.name(), String.valueOf(id));
                ProcessInfo pi = null;
                Thread isThread = null;
                Process process;
                int pId;
                try {
                    try {
                        synchronized (jobSet) {
                            if (closed) {
                                ji.getSubmitChannel().sendEvent(Event.shutdown, runningUser);
                                ji.getSubmitChannel().sendEvent(Event.retcode, RetCode.CANCELLED.getCode());
                                return;
                            }
                            process = pb.start();
                            isThread = Miscellaneous.pipeAsynchronously(ji.getSubmitChannel().getStdinIs(), (ErrorHandler) null, true, process.getOutputStream());
                            pId = Miscellaneous.getUnixId(process);
                            pi = new ProcessInfo(ji, pId);
                            ji.getSubmitChannel().sendEvent(Event.running, pId);
                            processMap.put(ji.getId(), pi);
                            int[] positions = getRunningPosition(pi);
                            if (positions == null) {
                                throw new AssertionError();
                            }
                            int groupCount = getGroupsRunning();
                            if (positions[1] >= groupCount) {
                                groupCount = positions[1] + 1;
                            }
                            pi.setNiceness(NicenessHandler.getInstance().getNiceness(positions[0], jobSet.countRunning(), positions[1], groupCount, Config.getInstance().getProcessCfg().getNicenessRange()[0], Config.getInstance().getProcessCfg().getNicenessRange()[1]));
                        }
                    } catch (Exception ex) {
                        ji.getSubmitChannel().sendEvent(Event.error, JsonCodec.getInstance().transform(Miscellaneous.getStrackTrace(ex)));
                        ji.getSubmitChannel().sendEvent(Event.retcode, RetCode.ERROR.getCode());
                        return;
                    }
                    Thread stoutReaderThread = Miscellaneous.pipeAsynchronously(process.getInputStream(), (ErrorHandler) null, true, ji.getSubmitChannel().getStdoutOs());
                    stoutReaderThread.setName("stdout-pid-" + pId);
                    Thread sterrReaderThread = Miscellaneous.pipeAsynchronously(process.getErrorStream(), (ErrorHandler) null, true, ji.getSubmitChannel().getStderrOs());
                    sterrReaderThread.setName("stderr-pid-" + pId);
                    try {
                        int code = process.waitFor();
                        isThread.interrupt();
                        if (!ji.isRelaunched()) {
                            ji.getSubmitChannel().sendEvent(Event.maxrss, pi.getMaxRss());
                            ji.getSubmitChannel().sendEvent(Event.maxswap, pi.getMaxSwap());
                            ji.getSubmitChannel().sendEvent(Event.retcode, code);
                        }
                    } catch (InterruptedException ex) {
                        try {
                            LinuxCommands.getInstance().killTree(pId);
                        } catch (Throwable th) {
                            LOGGER.log(Level.SEVERE, th.getMessage(), th);
                        }
                    } finally {
                        try {
                            stoutReaderThread.join();
                            sterrReaderThread.join();
                        } catch (Throwable th) {
                            LOGGER.log(Level.SEVERE, th.getMessage(), th);
                        }
                    }
                } finally {
                    try {
                        synchronized (jobSet) {
                            removeFromJobMap(ji);
                            jobSet.remove(id);
                            processMap.remove(id);
                            final GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                            gi.getJobs().remove(id);
                            if (gi.getJobs().isEmpty()) {
                                if (gi.getTimeToIdelSeconds() == 0) {
                                    groupMap.remove(gi.getGroupName());
                                } else if (gi.getTimeToIdelSeconds() > 0 && !ji.isRelaunched()) {
                                    Thread t = new Thread(coreGroup, "group-" + gi.getGroupName() + " idle thread") {
                                        @Override
                                        public void run() {
                                            try {
                                                Thread.sleep(1000 * gi.getTimeToIdelSeconds());
                                                synchronized (jobSet) {
                                                    if (gi.getJobs().isEmpty()) {
                                                        groupMap.remove(gi.getGroupName());

                                                    }
                                                }
                                            } catch (Throwable th) {
                                                if (th instanceof InterruptedException) {
                                                    return;
                                                }
                                                LOGGER.log(Level.SEVERE, th.getMessage(), th);
                                            }
                                        }
                                    };
                                    t.setDaemon(true);
                                    t.start();
                                }
                            }
                            if (ji.isRelaunched()) {
                                submit(ji.getSubmitChannel());
                            } else {
                                ji.getSubmitChannel().close();
                            }
                        }
                        LinuxCommands.getInstance().removeJobMemoryCgroup(id);
                    } catch (Throwable th) {
                        LOGGER.log(Level.SEVERE, th.getMessage(), th);
                    }
                }
            }
        };
        t.start();
    }

    public boolean close(PeerChannel<String> channel) throws IOException {
        if (!channel.getUser().equals("root") && !channel.getUser().equals(runningUser)) {
            channel.log(ANSICode.RED, "user '" + channel.getUser() + "' is not allowed to stop the core scheduler process");
            channel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
            channel.close();
            return false;
        }
        if (channel.getRequest() == null || channel.getRequest().isEmpty()) {
            channel.log(ANSICode.CYAN, "Confirm stopping scheduler by running: " + ANSICode.GREEN + "wava -x " + exitToken);
            channel.close();
            return false;
        } else if (!channel.getRequest().equals(exitToken)) {
            channel.log(ANSICode.RED, "Invalid token. Confirm stopping scheduler by running: " + ANSICode.GREEN + "wava -x " + exitToken);
            channel.close();
            return false;
        }
        channel.log(ANSICode.GREEN, "Stopping scheduler process ...");

        synchronized (jobSet) {
            this.closed = true;
            this.coreGroup.interrupt();

            Iterator<Integer> it = jobSet.getQueue();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                it.remove();
                removeFromJobMap(ji);
                GroupInfo gi = groupMap.get(ji.getSubmitChannel().getRequest().getGroupName());
                gi.getJobs().remove(id);
                try {
                    ji.getSubmitChannel().sendEvent(Event.shutdown, runningUser);
                    ji.getSubmitChannel().sendEvent(Event.retcode, RetCode.ERROR.getCode());
                    ji.getSubmitChannel().close();
                } catch (IOException ex) {
                    LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                }
            }

            it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                ProcessInfo pi;
                while ((pi = processMap.get(id)) == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                }
                pi.getJobInfo().getSubmitChannel().sendEvent(Event.shutdown, runningUser);
                try {
                    LinuxCommands.getInstance().killTree(pi.getPid());
                } catch (Exception ex) {
                    LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                }
            }
        }
        channel.sendEvent(Event.retcode, 0);
        channel.close();
        return true;
    }

    public class GroupInfo implements Comparable<GroupInfo> {

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

        @Override
        public int compareTo(GroupInfo o) {
            if (o == null) {
                return 1;
            }
            int ret = Integer.compare(priority, o.getPriority());
            if (ret == 0) {
                ret = Integer.compare(groupId, o.getGroupId());
            }
            return ret;
        }
    }

    public class JobInfo {

        private final int id;
        private final PeerChannel<ExtendedSubmitInput> submitChannel;

        private int previousQueuePosition;
        private volatile int queuedChildCount;
        private volatile int runningChildCount;
        private volatile boolean relaunched;

        public JobInfo(int id, PeerChannel<ExtendedSubmitInput> submitChannel) throws IOException, InterruptedException {
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

        public PeerChannel<ExtendedSubmitInput> getSubmitChannel() {
            return submitChannel;
        }

        public boolean isRelaunched() {
            return relaunched;
        }

        public void setRelaunched(boolean relaunched) {
            this.relaunched = relaunched;
        }

        public int getQueuedChildCount() {
            return queuedChildCount;
        }

        public void setQueuedChildCount(int queuedChildCount) {
            this.queuedChildCount = queuedChildCount;
        }

        public int getRunningChildCount() {
            return runningChildCount;
        }

        public void setRunningChildCount(int runningChildCount) {
            this.runningChildCount = runningChildCount;
        }
    }

    public class ProcessInfo {

        private final JobInfo jobInfo;
        private final int pId;
        private volatile long rss;
        private volatile long maxRss;
        private volatile long swap;
        private volatile long maxSwap;
        private volatile double cpuUsage;
        private volatile int niceness = Integer.MAX_VALUE;
        private boolean allowed;

        public ProcessInfo(JobInfo jobInfo, int pId) {
            this.jobInfo = jobInfo;
            this.pId = pId;
        }

        public long getRss() {
            return rss;
        }

        public void setRss(long rss) {
            this.rss = rss;
        }

        public long getMaxRss() {
            return maxRss;
        }

        public void setMaxRss(long maxRss) {
            this.maxRss = maxRss;
        }

        public long getMaxSwap() {
            return maxSwap;
        }

        public void setMaxSwap(long maxSwap) {
            this.maxSwap = maxSwap;
        }

        public double getCpuUsage() {
            return cpuUsage;
        }

        public void setCpuUsage(double cpuUsage) {
            this.cpuUsage = cpuUsage;
        }

        public int getPid() {
            return pId;
        }

        public long getSwap() {
            return swap;
        }

        public void setSwap(long swap) {
            this.swap = swap;
        }

        public int getNiceness() {
            return niceness;
        }

        public JobInfo getJobInfo() {
            return jobInfo;
        }

        public boolean isAllowed() {
            return allowed;
        }

        public void setAllowed(boolean allowed) {
            this.allowed = allowed;
        }

        public synchronized void setNiceness(int niceness) {
            if (niceness != this.niceness) {
                LinuxCommands.getInstance().setNiceness(pId, niceness);
                if (jobInfo.getSubmitChannel().getRequest().getParentId() != null) {
                    jobInfo.getSubmitChannel().sendEvent(Event.niceness, niceness);
                }
                this.niceness = niceness;
            }
        }
    }
}
