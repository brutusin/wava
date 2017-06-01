package org.brutusin.wava.core;

import java.io.File;
import org.brutusin.wava.io.Event;
import org.brutusin.wava.core.io.PeerChannel;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.utils.LinuxCommands;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.lang3.StringUtils;
import org.brutusin.commons.utils.ErrorHandler;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.cfg.GroupCfg;
import org.brutusin.wava.core.plug.NicenessHandler;
import org.brutusin.wava.core.stats.CpuStats;
import org.brutusin.wava.core.stats.IOStats;
import org.brutusin.wava.core.stats.MemoryStats;
import org.brutusin.wava.env.EnvEntry;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.ExtendedSubmitInput;
import org.brutusin.wava.input.ListJobsInput;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.utils.NonRootUserException;
import org.brutusin.wava.io.RetCode;

public class Scheduler {

    public final static String DEFAULT_GROUP_NAME = "default";
    public final static int EVICTION_ETERNAL = -1;

    private final static Logger LOGGER = Logger.getLogger(Scheduler.class.getName());

    private final Logger statsLogger;

    // next four accessed under synchronized(jobSet)
    private final JobSet jobSet = new JobSet();
    private final Map<Integer, JobInfo> jobMap = new HashMap<>();
    private final Map<Integer, ProcessInfo> processMap = new HashMap<>();
    private final Map<String, GroupInfo> groupMap = new HashMap<>();

    private final ThreadGroup coreGroup = new ThreadGroup(Scheduler.class.getName());
    private final ThreadGroup processGroup = new ThreadGroup(Scheduler.class.getName() + " processes");

    private final AtomicInteger jobCounter = new AtomicInteger();
    private final AtomicInteger groupCounter = new AtomicInteger();
    private final Thread cleaningThread;
    private final Thread statsThread;

    private final long totalManagedRss;
    private final long maxJobRss;

    private final String runningUser;

    private volatile boolean closed;
    private volatile String jobList;

    private StatRecord previousStatRecord;

    public Scheduler() throws NonRootUserException {

        this.runningUser = LinuxCommands.getRunningUser();
        if (!this.runningUser.equals("root")) {
            throw new NonRootUserException();
        }

        if (Config.getInstance().getSchedulerCfg().isLogStats()) {
            try {
                this.statsLogger = createStatsLogger(new File(Config.getInstance().getSchedulerCfg().getLogFolder(), "stats"));
                writeGlobalStatsFileHeader();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            this.statsLogger = null;
        }
        this.totalManagedRss = Miscellaneous.parseHumanReadableByteCount(Config.getInstance().getSchedulerCfg().getSchedulerCapacity());
        this.maxJobRss = Miscellaneous.parseHumanReadableByteCount(Config.getInstance().getSchedulerCfg().getMaxJobSize());
        boolean cgroupsCreated = LinuxCommands.createWavaCgroups(totalManagedRss);
        if (!cgroupsCreated) {
            throw new RuntimeException("Unable to create wava cgroups");
        }
        createGroup(DEFAULT_GROUP_NAME, this.runningUser, 0, EVICTION_ETERNAL);
        GroupCfg.Group[] predefinedGroups = Config.getInstance().getGroupCfg().getPredefinedGroups();
        if (predefinedGroups != null) {
            for (GroupCfg.Group group : predefinedGroups) {
                createGroup(group.getName(), this.runningUser, group.getPriority(), group.getTimeToIdleSeconds());
            }
        }

        this.jobList = createJobList(false, getAvailableManagedMemory(0), 0, new GaugeStats());

        this.cleaningThread = new Thread(this.coreGroup, "cleaningThread") {
            @Override
            public void run() {
                while (true) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    try {
                        Thread.sleep(Config.getInstance().getSchedulerCfg().getCleaningMillisecs());
                        cleanStalePeers();
                    } catch (Throwable th) {
                        if (th instanceof InterruptedException) {
                            break;
                        }
                        LOGGER.log(Level.SEVERE, th.getMessage(), th);
                    }
                }
            }
        };

        this.statsThread = new Thread(this.coreGroup, "statsThread") {
            @Override
            public void run() {
                while (true) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    try {
                        Thread.sleep(Config.getInstance().getSchedulerCfg().getStatsMillisecs());
                        takeStats();
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

    private boolean isWriteStatRecord(StatRecord previous, StatRecord current) {
        if (previous == null) {
            return true;
        }
        if (previous.running != current.running || previous.queded != current.queded) {
            return true;
        }
        if (Math.abs(previous.cpu - current.cpu) > Config.getInstance().getSchedulerCfg().getStatsCpuStep()
                || previous.cpu == 0 && current.cpu != 0
                || previous.cpu != 0 && current.cpu == 0) {
            return true;
        }
        if (Math.abs(previous.rss - current.rss) > Config.getInstance().getSchedulerCfg().getStatsRssStep()
                || previous.rss == 0 && current.rss != 0
                || previous.rss != 0 && current.rss == 0) {
            return true;
        }
        if (Math.abs(previous.swap - current.swap) > Config.getInstance().getSchedulerCfg().getStatsSwapStep()
                || previous.swap == 0 && current.swap != 0
                || previous.swap != 0 && current.swap == 0) {
            return true;
        }
        if (Math.abs(previous.io - current.io) > Config.getInstance().getSchedulerCfg().getStatsIOStep()
                || previous.io == 0 && current.io != 0
                || previous.io != 0 && current.io == 0) {
            return true;
        }
        return false;
    }

    private void writeGlobalStatsFileHeader() throws IOException {
        this.statsLogger.log(Level.ALL, "#start       \tend          \trunning\tqueued\tcpu(%)\trss(B)\tswap(B)\tio(B/s)");
    }

    private void writeGlobalStatsRecord(StatRecord rec) throws IOException {
        this.statsLogger.log(Level.ALL, String.format("%1$d\t%2$d\t%3$d\t%4$d\t%5$.1f\t%6$d\t%7$d\t%8$d", rec.start, rec.end, rec.running, rec.queded, rec.cpu, rec.rss, rec.swap, rec.io));
    }

    private void writeJobStatsFileHeader(Logger logger) throws IOException {
        logger.log(Level.ALL, "#start       \tend          \tcpu(%)\trss(B)\tswap(B)\tio(B/s)");
    }

    private void writeJobStatsRecord(Logger logger, StatRecord rec) throws IOException {
        logger.log(Level.ALL, String.format("%1$d\t%2$d\t%3$.1f\t%4$d\t%5$d\t%6$d", rec.start, rec.end, rec.cpu, rec.rss, rec.swap, rec.io));
    }

    public void start() {
        if (closed) {
            throw new IllegalStateException("Instance is closed");
        }
        this.cleaningThread.start();
        this.statsThread.start();
    }

    private GroupInfo createGroup(String name, String user, Integer priority, Integer timetoIdleSeconds) {
        LinuxCommands.createGroupCgroups(name);
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

    private void deleteGroup(String name) {
        synchronized (jobSet) {
            LinuxCommands.removeGroupCgroups(name);
            GroupInfo gi = groupMap.get(name);
            groupMap.remove(name);
        }
    }

    private void killForStarvationProtection(ProcessInfo pi) {
        try {
            if (pi.getJobInfo().getSubmitChannel().getInput().isIdempotent()) {
                LOGGER.log(Level.WARNING, "Starvation scenario found. Ralaunching idempotent job {0} ({1})", new Object[]{pi.getJobInfo().getId(), pi.getJobInfo().getSubmitChannel().getInput().getGroupName()});
                pi.getJobInfo().getSubmitChannel().sendEvent(Event.starvation_relaunch, runningUser);
                pi.getJobInfo().setRelaunched(true);
            } else {
                LOGGER.log(Level.SEVERE, "Starvation scenario found. Killing non-idempotent job {0} ({1})", new Object[]{pi.getJobInfo().getId(), pi.getJobInfo().getSubmitChannel().getInput().getGroupName()});
                pi.getJobInfo().getSubmitChannel().sendEvent(Event.starvation_stop, runningUser);
            }
            LinuxCommands.killTree(pi.getPid());
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    private long getAllocatedManagedMemory() {
        synchronized (jobSet) {
            long sum = 0;
            JobSet.RunningIterator it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                sum += ji.getSubmitChannel().getInput().getMaxRSS();
            }
            return sum;
        }
    }

    private void cleanStalePeers() throws IOException, InterruptedException {
        synchronized (jobSet) {
            Iterator<Integer> it = jobSet.getQueue();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                if (!ji.getSubmitChannel().isPeerAlive()) {
                    it.remove();
                    removeFromJobMap(ji);
                    GroupInfo gi = groupMap.get(ji.getSubmitChannel().getInput().getGroupName());
                    gi.getJobs().remove(id);
                    try {
                        ji.getSubmitChannel().close();
                    } catch (IOException ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                    onStateChanged();
                }
            }

            it = jobSet.getRunning();
            while (it.hasNext()) {
                Integer id = it.next();
                ProcessInfo pi = processMap.get(id);
                if (pi != null && !pi.getJobInfo().getSubmitChannel().isPeerAlive()) {
                    try {
                        LinuxCommands.killTree(pi.getPid());
                    } catch (RuntimeException ex) {
                        LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                    }
                }
            }
        }
    }

    private void sendQueuePositionEventsToParentJobs() {
        synchronized (jobSet) {
            int position = 0;
            JobSet.QueueIterator it = jobSet.getQueue();
            while (it.hasNext()) {
                position++;
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                if (ji.getSubmitChannel().getInput().getParentId() == null && position != ji.getPreviousQueuePosition()) {
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
                    if (prev == null || !prev.getJobInfo().getSubmitChannel().getInput().getGroupName().equals(pi.getJobInfo().getSubmitChannel().getInput().getGroupName())) {
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
                    if (prev != null && !prev.getJobInfo().getSubmitChannel().getInput().getGroupName().equals(pi.getJobInfo().getSubmitChannel().getInput().getGroupName())) {
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
                    maxRSsSumOfBlockedJobs += ji.getSubmitChannel().getInput().getMaxRSS();
                    ProcessInfo pi = processMap.get(id);
                    if (pi != null) {
                        if (pi.getJobInfo().isRelaunched()) {
                            maxRSsSumOfBlockedJobs -= ji.getSubmitChannel().getInput().getMaxRSS();
                            continue;
                        }
                        if (candidateToKill == null || !candidateToKill.getJobInfo().getSubmitChannel().getInput().isIdempotent() || ji.getSubmitChannel().getInput().isIdempotent()) {
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
                    if (maxRSsSumOfBlockedJobs + firstQueued.getSubmitChannel().getInput().getMaxRSS() > this.totalManagedRss) {
                        killForStarvationProtection(candidateToKill);
                    }
                }
            }
        }
    }

    private void dequeueJobs() {
        long availableMemory = getAvailableManagedMemory(getAllocatedManagedMemory());
        synchronized (jobSet) {
            JobSet.QueueIterator queuedIt = jobSet.getQueue();
            while (queuedIt.hasNext()) {
                Integer id = queuedIt.next();
                JobInfo ji = jobMap.get(id);
                if (ji.getSubmitChannel().getInput().getMaxRSS() > availableMemory) {
                    return;
                }
                queuedIt.moveToRunning();
                changeQueuedChildren(ji.getSubmitChannel().getInput().getParentId(), false);
                changeRunningChildren(ji.getSubmitChannel().getInput().getParentId(), true);
                execute(id, ji);
                availableMemory -= ji.getSubmitChannel().getInput().getMaxRSS();
            }
        }
    }

    private long getAvailableManagedMemory(long allocatedManagedMemory) {
        long availableManagedMemory = totalManagedRss - allocatedManagedMemory;
        long systemAvailableMemory = LinuxCommands.getMemInfo()[1];
        if (systemAvailableMemory < 0) { // Unable to get available memory
            return availableManagedMemory;
        }
        if (availableManagedMemory > systemAvailableMemory) {
            availableManagedMemory = systemAvailableMemory;
        }
        return availableManagedMemory;
    }

    private void onStateChanged() throws IOException, InterruptedException {
        distributeNiceness();
        checkStarvation();
        dequeueJobs();
        sendQueuePositionEventsToParentJobs();
        takeStats();
    }

    private void takeStats() throws IOException, InterruptedException {
        synchronized (jobSet) {
            long allocatedManagedMemory = getAllocatedManagedMemory();
            long availableManagedMemory = getAvailableManagedMemory(allocatedManagedMemory);
            GaugeStats stats = getStats();
            this.jobList = createJobList(false, availableManagedMemory, allocatedManagedMemory, stats);
            long end = System.currentTimeMillis();
            if (statsLogger != null) {
                StatRecord record = new StatRecord();
                record.running = jobSet.countRunning();
                record.queded = jobSet.countQueued();
                record.cpu = stats.cpuGaugeStats.getCpuPercent();
                record.rss = stats.memStats.rssBytes;
                record.swap = stats.memStats.swapBytes;
                record.io = stats.iOGaugeStats.ioBps;
                if (isWriteStatRecord(previousStatRecord, record)) {
                    if (previousStatRecord != null) {
                        record.start = previousStatRecord.end;
                    } else {
                        record.start = end;
                    }
                    record.end = end;
                    writeGlobalStatsRecord(record);
                    previousStatRecord = record;
                }
            }
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
                if (prev != null && !prev.getJobInfo().getSubmitChannel().getInput().getGroupName().equals(p.getJobInfo().getSubmitChannel().getInput().getGroupName())) {
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

    private GaugeStats getStats() throws IOException, InterruptedException {
        synchronized (jobSet) {
            GaugeStats ret = new GaugeStats();
            Iterator<Integer> running = jobSet.getRunning();
            while (running.hasNext()) {
                Integer id = running.next();
                ProcessInfo pi = processMap.get(id);
                if (pi == null) {
                    continue;
                }
                MemoryStats memStats = LinuxCommands.getCgroupMemoryStats(pi.getJobInfo().getSubmitChannel().getInput().getGroupName(), id);
                if (memStats == null) {
                    continue;
                }
                CpuStats cpuStats = LinuxCommands.getCgroupCpuStats(pi.getJobInfo().getSubmitChannel().getInput().getGroupName(),id);
                if (cpuStats == null) {
                    continue;
                }
                IOStats ioStats = LinuxCommands.getCgroupIOStats(pi.getJobInfo().getSubmitChannel().getInput().getGroupName(), id);
                if (ioStats == null) {
                    continue;
                }
                pi.setCurrentStats(new Statistics(memStats, cpuStats, ioStats));
                ret.cpuGaugeStats.systemCpuPercent += pi.getGaugeStats().cpuGaugeStats.systemCpuPercent;
                ret.cpuGaugeStats.userCpuPercent += pi.getGaugeStats().cpuGaugeStats.userCpuPercent;
                ret.memStats.rssBytes += pi.getGaugeStats().memStats.rssBytes;
                ret.memStats.swapBytes += pi.getGaugeStats().memStats.swapBytes;
                ret.iOGaugeStats.ioBps += pi.getGaugeStats().iOGaugeStats.ioBps;

                if (pi.getStatsLogger() != null) {
                    StatRecord record = new StatRecord();
                    record.cpu = pi.getGaugeStats().cpuGaugeStats.getCpuPercent();
                    record.rss = pi.getGaugeStats().memStats.rssBytes;
                    record.swap = pi.getGaugeStats().memStats.swapBytes;
                    record.io = pi.getGaugeStats().iOGaugeStats.ioBps;
                    long time = System.currentTimeMillis();
                    if (isWriteStatRecord(pi.getPreviousStatRecord(), record)) {
                        if (pi.getPreviousStatRecord() != null) {
                            record.start = pi.getPreviousStatRecord().end;
                        } else {
                            record.start = time;
                        }
                        record.end = time;
                        writeJobStatsRecord(pi.getStatsLogger(), record);
                        pi.setPreviousStatRecord(record);
                    }
                }
            }
            return ret;
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

        if (maxJobRss > 0 && submitChannel.getInput().getMaxRSS() > maxJobRss) {
            submitChannel.getInput().setMaxRSS(maxJobRss);
        }
        if (totalManagedRss < submitChannel.getInput().getMaxRSS()) {
            submitChannel.getInput().setMaxRSS(totalManagedRss);
        }

        long treeRSS = submitChannel.getInput().getMaxRSS();
        Integer parentId = submitChannel.getInput().getParentId();

        while (parentId != null) {
            JobInfo ji = jobMap.get(parentId);
            if (ji == null) {
                break;
            }
            treeRSS += ji.getSubmitChannel().getInput().getMaxRSS();
            parentId = ji.getSubmitChannel().getInput().getParentId();
        }

        if (treeRSS > totalManagedRss) {
            submitChannel.sendEvent(Event.exceed_tree, treeRSS);
            submitChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
            submitChannel.close();
            return;
        }

        if (submitChannel.getInput().getGroupName() == null) {
            submitChannel.getInput().setGroupName(DEFAULT_GROUP_NAME);
        }

        synchronized (jobSet) {
            JobInfo ji = new JobInfo(jobCounter.incrementAndGet(), submitChannel);
            LOGGER.fine("Received job " + ji.getId() + ": " + Arrays.toString(ji.getSubmitChannel().getInput().getCommand()));
            GroupInfo gi = groupMap.get(ji.getSubmitChannel().getInput().getGroupName());
            if (gi == null) { // dynamic group
                gi = createGroup(ji.getSubmitChannel().getInput().getGroupName(), ji.getSubmitChannel().getUser(), 0, Config.getInstance().getGroupCfg().getDynamicGroupIdleSeconds());
            }
            gi.getJobs().add(ji.getId());
            changeQueuedChildren(ji.getSubmitChannel().getInput().getParentId(), true);
            jobMap.put(ji.getId(), ji);
            ji.getSubmitChannel().sendEvent(Event.id, ji.getId());
            jobSet.queue(ji.getId(), gi.getPriority(), gi.getGroupId());
            ji.getSubmitChannel().sendEvent(Event.priority, gi.getPriority());
            onStateChanged();
        }
    }

    private String createJobList(boolean noHeaders, long availableManagedMemory, long allocatedManagedMemory, GaugeStats stats) {
        StringBuilder sb = new StringBuilder(200);
        try {
            if (!noHeaders) {
                sb.append(ANSICode.BLACK.getCode());
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(ANSICode.BOLD.getCode());
                sb.append(" JOB INFO ");
                sb.append(ANSICode.RESET.getCode());
                sb.append(ANSICode.GREEN.getCode());
                String[] mem = Miscellaneous.humanReadableByteCount(allocatedManagedMemory, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                sb.append(StringUtils.leftPad(mem[0], 32));
                sb.append(" ");
                sb.append(StringUtils.rightPad(mem[1], 3));
                sb.append(" ");

                sb.append(ANSICode.BLACK.getCode());
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(ANSICode.BOLD.getCode());
                sb.append(" PROCESS TREE STATS ");
                sb.append(ANSICode.RESET.getCode());
                sb.append(ANSICode.GREEN.getCode());

                mem = Miscellaneous.humanReadableByteCount(stats.memStats.rssBytes, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                sb.append(StringUtils.leftPad(mem[0], 26));
                sb.append(" ");
                sb.append(StringUtils.rightPad(mem[1], 3));
                sb.append(" ");
                mem = Miscellaneous.humanReadableByteCount(stats.memStats.swapBytes, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                sb.append(StringUtils.leftPad(mem[0], 6));
                sb.append(" ");
                sb.append(StringUtils.rightPad(mem[1], 3));
                sb.append(" ");
                mem = Miscellaneous.humanReadableByteCount(stats.iOGaugeStats.ioBps, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                sb.append(StringUtils.leftPad(mem[0], 6));
                sb.append(" ");
                sb.append(StringUtils.rightPad(mem[1] + "/s", 5));
                sb.append(" ");
                sb.append(StringUtils.leftPad(String.format("%.1f", stats.cpuGaugeStats.getCpuPercent()), 6));
                sb.append(" ");
                sb.append(ANSICode.BLACK.getCode());
                sb.append(ANSICode.BG_GREEN.getCode());
                sb.append(ANSICode.BOLD.getCode());
                sb.append(" COMMAND ");
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
                sb.append(StringUtils.leftPad("NICE", 4));
                sb.append(" ");
                sb.append(StringUtils.leftPad("MAX_RSS", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("MAX_SWAP", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("MAX_IO   ", 12));
                sb.append(" ");
                sb.append(StringUtils.leftPad("RSS  ", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("SWAP  ", 10));
                sb.append(" ");
                sb.append(StringUtils.leftPad("IO     ", 12));
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
                    GroupInfo gi = groupMap.get(ji.getSubmitChannel().getInput().getGroupName());
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
                        if (ji.getSubmitChannel().getInput().getParentId() != null) {
                            pId = String.valueOf(ji.getSubmitChannel().getInput().getParentId());
                        } else {
                            pId = "";
                        }
                        sb.append(StringUtils.leftPad(pId, 8));
                        sb.append(" ");

                        sb.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                        sb.append(" ");

                        sb.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                        sb.append(" ");

                        String[] mem = Miscellaneous.humanReadableByteCount(ji.getSubmitChannel().getInput().getMaxRSS(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");

                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");

                        sb.append(StringUtils.leftPad(String.valueOf(pi.getNiceness()), 4));
                        sb.append(" ");

                        mem = Miscellaneous.humanReadableByteCount(pi.getMaxGaugeStats().memStats.rssBytes, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");

                        if (pi.getMaxGaugeStats().memStats.swapBytes > ji.getSubmitChannel().getInput().getMaxRSS() / 2) {
                            sb.append(ANSICode.RED.getCode());
                        } else if (pi.getMaxGaugeStats().memStats.swapBytes > 0) {
                            sb.append(ANSICode.YELLOW.getCode());
                        }
                        mem = Miscellaneous.humanReadableByteCount(pi.getMaxGaugeStats().memStats.swapBytes, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");

                        mem = Miscellaneous.humanReadableByteCount(pi.getMaxGaugeStats().iOGaugeStats.ioBps, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1] + "/s", 5));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");

                        mem = Miscellaneous.humanReadableByteCount(pi.getGaugeStats().memStats.rssBytes, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");

                        if (pi.getGaugeStats().memStats.swapBytes > ji.getSubmitChannel().getInput().getMaxRSS() / 2) {
                            sb.append(ANSICode.RED.getCode());
                        } else if (pi.getGaugeStats().memStats.swapBytes > 0) {
                            sb.append(ANSICode.YELLOW.getCode());
                        }
                        mem = Miscellaneous.humanReadableByteCount(pi.getGaugeStats().memStats.swapBytes, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");

                        mem = Miscellaneous.humanReadableByteCount(pi.getGaugeStats().iOGaugeStats.ioBps, Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1] + "/s", 5));
                        sb.append(ANSICode.RESET.getCode());
                        sb.append(" ");

                        sb.append(StringUtils.leftPad(String.format("%.1f", pi.getGaugeStats().cpuGaugeStats.getCpuPercent()), 6));
                        sb.append(" ");
                        sb.append(Arrays.toString(ji.getSubmitChannel().getInput().getCommand()));
                        sb.append(" ");
                    } else { // process not stated yet
                        sb.append(StringUtils.leftPad(String.valueOf(id), 8));
                        sb.append(" ");
                        String pId;
                        if (ji.getSubmitChannel().getInput().getParentId() != null) {
                            pId = String.valueOf(ji.getSubmitChannel().getInput().getParentId());
                        } else {
                            pId = "";
                        }
                        sb.append(StringUtils.leftPad(pId, 8));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(String.valueOf(gi.getGroupName()), 8));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                        sb.append(" ");
                        String[] mem = Miscellaneous.humanReadableByteCount(ji.getSubmitChannel().getInput().getMaxRSS(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                        sb.append(StringUtils.leftPad(mem[0], 6));
                        sb.append(" ");
                        sb.append(StringUtils.rightPad(mem[1], 3));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 4));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 12));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 10));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 12));
                        sb.append(" ");
                        sb.append(StringUtils.leftPad("", 6));
                        sb.append(" ");
                        sb.append(Arrays.toString(ji.getSubmitChannel().getInput().getCommand()));
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
                    if (ji.getSubmitChannel().getInput().getParentId() != null) {
                        pId = String.valueOf(ji.getSubmitChannel().getInput().getParentId());
                    } else {
                        pId = "";
                    }
                    sb.append(StringUtils.leftPad(pId, 8));
                    sb.append(" ");
                    sb.append(StringUtils.rightPad(String.valueOf(ji.getSubmitChannel().getInput().getGroupName()), 8));
                    sb.append(" ");
                    sb.append(StringUtils.rightPad(ji.getSubmitChannel().getUser(), 8));
                    sb.append(" ");
                    String[] mem = Miscellaneous.humanReadableByteCount(ji.getSubmitChannel().getInput().getMaxRSS(), Config.getInstance().getuICfg().issIMemoryUnits()).split(" ");
                    sb.append(StringUtils.leftPad(mem[0], 6));
                    sb.append(" ");
                    sb.append(StringUtils.rightPad(mem[1], 3));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 4));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 12));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 10));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 12));
                    sb.append(" ");
                    sb.append(StringUtils.leftPad("", 6));
                    sb.append(" ");
                    sb.append(Arrays.toString(ji.getSubmitChannel().getInput().getCommand()));
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
                statSb.append("  Available memory: ");
                statSb.append(ANSICode.GREEN);
                statSb.append(Miscellaneous.humanReadableByteCount(availableManagedMemory, Config.getInstance().getuICfg().issIMemoryUnits()));
                statSb.append(ANSICode.RESET);
                statSb.append(ANSICode.CYAN);
                statSb.append(" / ");
                statSb.append(Miscellaneous.humanReadableByteCount(totalManagedRss, Config.getInstance().getuICfg().issIMemoryUnits()));
                statSb.append("\n");
                statSb.append("\n");
                sb.insert(0, statSb);
            }
        } finally {
            ANSICode.setActive(true);
        }
        return sb.toString();
    }

    public void listGroups(PeerChannel<GroupInput> channel) throws IOException, InterruptedException {
        try {
            if (!channel.getInput().isNoHeaders()) {
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

    public void listJobs(PeerChannel<ListJobsInput> channel) throws IOException, InterruptedException {
        try {
            if (channel.getInput().isNoHeaders()) {
                long allocatedManagedMemory = getAllocatedManagedMemory();
                long availableManagedMemory = getAvailableManagedMemory(allocatedManagedMemory);
                PeerChannel.println(channel.getStdoutOs(), createJobList(true, availableManagedMemory, allocatedManagedMemory, getStats()));
            } else if (!closed) {
                PeerChannel.println(channel.getStdoutOs(), jobList);
            } else {
                long allocatedManagedMemory = getAllocatedManagedMemory();
                long availableManagedMemory = getAvailableManagedMemory(allocatedManagedMemory);
                PeerChannel.println(channel.getStdoutOs(), createJobList(false, availableManagedMemory, allocatedManagedMemory, getStats()));
            }
        } finally {
            channel.sendEvent(Event.retcode, 0);
            channel.close();
        }
    }

    private static Logger createStatsLogger(File folder) throws IOException {
        Logger logger = Logger.getAnonymousLogger();
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                logger.removeHandler(handler);
            }
        }
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        if (folder.exists()) {
            Miscellaneous.deleteDirectory(folder);
        }
        Miscellaneous.createDirectory(folder);
        int maxFiles = 10;
        int maxBytesPerFile = (int) (Config.getInstance().getSchedulerCfg().getMaxStatsLogSize() / maxFiles);
        FileHandler fh = new FileHandler(folder.getAbsolutePath() + "/stats%g.log", maxBytesPerFile, maxFiles, false);
        fh.setLevel(Level.ALL);
        SimpleFormatter formatter = new SimpleFormatter() {
            @Override
            public synchronized String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        };
        fh.setFormatter(formatter);
        logger.addHandler(fh);
        return logger;
    }

    private static void closeLogger(Logger logger) {
        if (logger == null) {
            return;
        }
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                handler.close();
            }
        }
    }

    public void cancel(PeerChannel<CancelInput> cancelChannel) throws IOException, InterruptedException {
        try {
            if (closed) {
                throw new IllegalStateException("Instance is closed");
            }
            int id = cancelChannel.getInput().getId();
            synchronized (jobSet) {
                JobSet.State state = jobSet.getState(id);
                if (state == null) {
                    cancelChannel.sendMessage(ANSICode.RED, "Job not found");
                    cancelChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                } else if (state == JobSet.State.queued) {
                    JobInfo ji = jobMap.get(id);
                    if (ji != null) {
                        if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(ji.getSubmitChannel().getUser())) {
                            cancelChannel.sendMessage(ANSICode.RED, "User '" + cancelChannel.getUser() + "' is not allowed to cancel a job from user '" + ji.getSubmitChannel().getUser() + "'");
                            cancelChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                            return;
                        }
                        ji.getSubmitChannel().sendEvent(Event.cancelled, cancelChannel.getUser());
                        ji.getSubmitChannel().sendEvent(Event.retcode, RetCode.CANCELLED.getCode());
                        ji.getSubmitChannel().close();
                        cancelChannel.sendMessage(ANSICode.GREEN, "Enqueued job sucessfully cancelled");
                        cancelChannel.sendEvent(Event.retcode, 0);
                        GroupInfo gi = groupMap.get(ji.getSubmitChannel().getInput().getGroupName());
                        gi.getJobs().remove(id);
                        jobSet.remove(id);
                        removeFromJobMap(ji);
                        LOGGER.fine("Cancelled job " + id + " by user '" + cancelChannel.getUser() + "'");
                    } else {
                        throw new AssertionError();
                    }
                    onStateChanged();
                } else if (state == JobSet.State.running) {
                    ProcessInfo pi = processMap.get(id);
                    if (pi != null) {
                        if (!cancelChannel.getUser().equals("root") && !cancelChannel.getUser().equals(pi.getJobInfo().getSubmitChannel().getUser())) {
                            cancelChannel.sendMessage(ANSICode.RED, "User '" + cancelChannel.getUser() + "' is not allowed to cancel a job from user '" + pi.getJobInfo().getSubmitChannel().getUser() + "'");
                            cancelChannel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                            return;
                        }
                        pi.getJobInfo().getSubmitChannel().sendEvent(Event.cancelled, cancelChannel.getUser());
                        LinuxCommands.killTree(pi.getPid());
                        cancelChannel.sendMessage(ANSICode.GREEN, "Running job sucessfully cancelled");
                        cancelChannel.sendEvent(Event.retcode, 0);
                        LOGGER.fine("Cancelled job " + id + " by user '" + cancelChannel.getUser() + "'");
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
                GroupInfo gi = groupMap.get(channel.getInput().getGroupName());
                if (gi == null) {
                    createGroup(channel.getInput().getGroupName(), channel.getUser(), channel.getInput().getPriority(), channel.getInput().getTimetoIdleSeconds());
                    channel.sendMessage(ANSICode.GREEN, "Group '" + channel.getInput().getGroupName() + "' created successfully");
                    channel.sendEvent(Event.retcode, 0);
                    LOGGER.fine("Group '" + channel.getInput().getGroupName() + "' created  by user '" + channel.getUser() + "'");
                    return;
                } else if (channel.getInput().isDelete()) {
                    if (!channel.getUser().equals("root") && !channel.getUser().equals(gi.getUser())) {
                        if (gi.getUser().equals("root")) {
                            channel.sendMessage(ANSICode.RED, "Group '" + channel.getInput().getGroupName() + "' can only be updated by user 'root'");
                        } else {
                            channel.sendMessage(ANSICode.RED, "Group '" + channel.getInput().getGroupName() + "' can only be updated by users 'root' and '" + gi.getUser() + "'");
                        }
                        channel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                        return;
                    }
                    if (gi.getJobs().isEmpty()) {
                        channel.sendMessage(ANSICode.GREEN, "Group '" + channel.getInput().getGroupName() + "' deleted successfully");
                        deleteGroup(channel.getInput().getGroupName());
                        channel.sendEvent(Event.retcode, 0);
                        LOGGER.fine("Group '" + channel.getInput().getGroupName() + "' deleted  by user " + channel.getUser() + "'");
                        return;
                    } else {
                        channel.sendMessage(ANSICode.RED, "Group '" + channel.getInput().getGroupName() + "' cannot be deleted, since it contains " + gi.getJobs().size() + " active jobs");
                        channel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                        return;
                    }
                }
                Integer newPriority = channel.getInput().getPriority();
                if (newPriority != null && newPriority != gi.getPriority()) {
                    synchronized (gi.getJobs()) {
                        for (Integer id : gi.getJobs()) {
                            jobSet.setPriority(id, newPriority, gi.getGroupId());
                            JobInfo ji = jobMap.get(id);
                            ji.getSubmitChannel().sendEvent(Event.priority, newPriority);
                        }
                    }
                    gi.setPriority(newPriority);
                    channel.sendMessage(ANSICode.GREEN, "Group '" + channel.getInput().getGroupName() + "' priority updated successfully");
                    LOGGER.fine("Group '" + channel.getInput().getGroupName() + "' priority updated by user '" + channel.getUser() + "'");
                }
                Integer newTimetoIdleSeconds = channel.getInput().getTimetoIdleSeconds();
                if (newTimetoIdleSeconds != null && newTimetoIdleSeconds != gi.getTimeToIdelSeconds()) {
                    gi.setTimeToIdelSeconds(newTimetoIdleSeconds);
                    channel.sendMessage(ANSICode.GREEN, "Group '" + channel.getInput().getGroupName() + "' time-to-idle updated successfully");
                    LOGGER.fine("Group '" + channel.getInput().getGroupName() + "' time-to-idle updated by user '" + channel.getUser() + "'");
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
            changeQueuedChildren(jobInfo.getSubmitChannel().getInput().getParentId(), false);
        } else {
            changeRunningChildren(jobInfo.getSubmitChannel().getInput().getParentId(), false);
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
        LinuxCommands.createJobCgroups(ji.getSubmitChannel().getInput().getGroupName(), id, ji.getSubmitChannel().getInput().getMaxRSS());
        Thread t = new Thread(this.processGroup, "scheduled process " + id) {
            @Override
            public void run() {
                String[] cmd = ji.getSubmitChannel().getInput().getCommand();
                cmd = LinuxCommands.decorateRunAsCommand(cmd, ji.getSubmitChannel().getUser());
                cmd = LinuxCommands.decorateWithCPUAffinity(cmd, Config.getInstance().getProcessCfg().getCpuAfinity());
                cmd = LinuxCommands.decorateWithBatchSchedulerPolicy(cmd);
                cmd = LinuxCommands.decorateRunInCgroup(cmd, ji.getSubmitChannel().getInput().getGroupName(), id);
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.environment().clear();
                pb.directory(ji.getSubmitChannel().getInput().getWorkingDirectory());
                if (ji.getSubmitChannel().getInput().getEnvironment() != null) {
                    pb.environment().putAll(ji.getSubmitChannel().getInput().getEnvironment());
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
                            LOGGER.fine("Running job " + ji.getId() + " with pId " + pId);

                            Logger statsLogger;
                            if (ji.getSubmitChannel().getInput().getStatsDirectory() != null) {
                                statsLogger = createStatsLogger(ji.getSubmitChannel().getInput().getStatsDirectory());
                                writeJobStatsFileHeader(statsLogger);
                            } else {
                                statsLogger = null;
                            }
                            pi = new ProcessInfo(ji, pId, statsLogger);
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
                            if (pi.getMaxGaugeStats() != null) {
                                ji.getSubmitChannel().sendEvent(Event.maxrss, pi.getMaxGaugeStats().memStats.rssBytes);
                                ji.getSubmitChannel().sendEvent(Event.maxswap, pi.getMaxGaugeStats().memStats.swapBytes);
                            }
                            ji.getSubmitChannel().sendEvent(Event.retcode, code);
                            closeLogger(pi.getStatsLogger());
                        }
                    } catch (InterruptedException ex) {
                        try {
                            LinuxCommands.killTree(pId);
                        } catch (Throwable th) {
                            LOGGER.log(Level.SEVERE, th.getMessage(), th);
                        }
                    } finally {
                        try {
                            stoutReaderThread.join();
                            sterrReaderThread.join();
                            isThread.join();
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
                            final GroupInfo gi = groupMap.get(ji.getSubmitChannel().getInput().getGroupName());
                            gi.getJobs().remove(id);
                            if (gi.getJobs().isEmpty()) {
                                if (gi.getTimeToIdelSeconds() == 0) {
                                    deleteGroup(gi.getGroupName());
                                } else if (gi.getTimeToIdelSeconds() > 0 && !ji.isRelaunched()) {
                                    Thread t = new Thread(coreGroup, "group-" + gi.getGroupName() + " idle thread") {
                                        @Override
                                        public void run() {
                                            try {
                                                Thread.sleep(1000 * gi.getTimeToIdelSeconds());
                                                synchronized (jobSet) {
                                                    if (gi.getJobs().isEmpty()) {
                                                        deleteGroup(gi.getGroupName());

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
                                LOGGER.fine("Closing channel of job " + ji.getId());
                                ji.getSubmitChannel().close();
                            }
                            onStateChanged();
                        }
                        LinuxCommands.removeJobCgroups(ji.getSubmitChannel().getInput().getGroupName(), id);
                    } catch (Throwable th) {
                        LOGGER.log(Level.SEVERE, th.getMessage(), th);
                    }
                }
            }
        };
        t.start();
    }

    public boolean close(PeerChannel<?> channel) throws IOException {
        if (!channel.getUser().equals("root") && !channel.getUser().equals(runningUser)) {
            channel.sendMessage(ANSICode.RED, "User '" + channel.getUser() + "' is not allowed to stop the core scheduler process");
            channel.sendEvent(Event.retcode, RetCode.ERROR.getCode());
            channel.close();
            return false;
        }

        channel.sendMessage(ANSICode.GREEN, "Stopping scheduler process ...");

        this.cleaningThread.interrupt();
        this.statsThread.interrupt();

        synchronized (jobSet) {
            this.closed = true;
            this.coreGroup.interrupt();

            Iterator<Integer> it = jobSet.getQueue();
            while (it.hasNext()) {
                Integer id = it.next();
                JobInfo ji = jobMap.get(id);
                it.remove();
                removeFromJobMap(ji);
                GroupInfo gi = groupMap.get(ji.getSubmitChannel().getInput().getGroupName());
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
                    LinuxCommands.killTree(pi.getPid());
                } catch (Exception ex) {
                    LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                }
            }
        }
        String msg = "Scheduler stopped sucessfully";
        LOGGER.severe(msg);
        channel.sendMessage(ANSICode.GREEN, msg);
        channel.sendEvent(Event.retcode, 0);
        channel.close();
        closeLogger(this.statsLogger);
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
        private final Logger statsLogger;
        private StatRecord previousStatRecord;
        private volatile int niceness = Integer.MAX_VALUE;
        private volatile GaugeStats maxGaugeStats = new GaugeStats();
        private volatile GaugeStats gaugeStats = new GaugeStats();
        private volatile Statistics prevStats;
        private volatile Statistics currentStats;

        public ProcessInfo(JobInfo jobInfo, int pId, Logger statsLogger) {
            this.jobInfo = jobInfo;
            this.pId = pId;
            this.statsLogger = statsLogger;
        }

        public int getPid() {
            return pId;
        }

        public int getNiceness() {
            return niceness;
        }

        public JobInfo getJobInfo() {
            return jobInfo;
        }

        public Logger getStatsLogger() {
            return statsLogger;
        }

        public StatRecord getPreviousStatRecord() {
            return previousStatRecord;
        }

        public void setPreviousStatRecord(StatRecord previousStatRecord) {
            this.previousStatRecord = previousStatRecord;
        }

        public synchronized void setNiceness(int niceness) {
            if (niceness != this.niceness) {
                LinuxCommands.setNiceness(pId, niceness);
                if (jobInfo.getSubmitChannel().getInput().getParentId() != null) {
                    jobInfo.getSubmitChannel().sendEvent(Event.niceness, niceness);
                }
                this.niceness = niceness;
            }
        }

        public Statistics getPrevStats() {
            return prevStats;
        }

        public Statistics getCurrentStats() {
            return currentStats;
        }

        public GaugeStats getMaxGaugeStats() {
            return maxGaugeStats;
        }

        public GaugeStats getGaugeStats() {
            return gaugeStats;
        }

        public void setCurrentStats(Statistics currentStats) {
            this.prevStats = this.currentStats;
            this.currentStats = currentStats;
            if (prevStats != null) {
                this.gaugeStats = new GaugeStats(prevStats, currentStats);
                if (this.maxGaugeStats == null) {
                    this.maxGaugeStats = this.gaugeStats;
                } else {
                    if (this.maxGaugeStats.cpuGaugeStats.getCpuPercent() < this.gaugeStats.cpuGaugeStats.getCpuPercent()) {
                        this.maxGaugeStats.cpuGaugeStats = this.gaugeStats.cpuGaugeStats;
                    }
                    if (this.maxGaugeStats.memStats.rssBytes < this.gaugeStats.memStats.rssBytes) {
                        this.maxGaugeStats.memStats.rssBytes = this.gaugeStats.memStats.rssBytes;
                    }
                    if (this.maxGaugeStats.memStats.swapBytes < this.gaugeStats.memStats.swapBytes) {
                        this.maxGaugeStats.memStats.swapBytes = this.gaugeStats.memStats.swapBytes;
                    }
                    if (this.maxGaugeStats.iOGaugeStats.ioBps < this.gaugeStats.iOGaugeStats.ioBps) {
                        this.maxGaugeStats.iOGaugeStats.ioBps = this.gaugeStats.iOGaugeStats.ioBps;
                    }
                }
            }
        }
    }

    public static class Statistics {

        public MemoryStats memStats;
        public CpuStats cpuStats;
        public IOStats iOStats;

        public Statistics(MemoryStats memStats, CpuStats cpuStats, IOStats iOStats) {
            this.memStats = memStats;
            this.cpuStats = cpuStats;
            this.iOStats = iOStats;
        }
    }

    public static class GaugeStats {

        public MemoryStats memStats;
        public CpuGaugeStats cpuGaugeStats;
        public IOGaugeStats iOGaugeStats;

        public GaugeStats() {
            this.memStats = new MemoryStats();
            this.cpuGaugeStats = new CpuGaugeStats();
            this.iOGaugeStats = new IOGaugeStats();
        }

        public GaugeStats(Statistics prevStats, Statistics currentStats) {
            this.memStats = currentStats.memStats;
            this.cpuGaugeStats = new CpuGaugeStats();
            this.iOGaugeStats = new IOGaugeStats();
            if (currentStats.cpuStats.nanos > prevStats.cpuStats.nanos && prevStats.cpuStats.nanos > 0) {
                double denom = (LinuxCommands.getUserHz() / 100.0) * (currentStats.cpuStats.nanos - prevStats.cpuStats.nanos) / 1e9;
                this.cpuGaugeStats.userCpuPercent = (currentStats.cpuStats.userJiffies - prevStats.cpuStats.userJiffies) / denom;
                this.cpuGaugeStats.systemCpuPercent = (currentStats.cpuStats.systemJiffies - prevStats.cpuStats.systemJiffies) / denom;
            }
            if (currentStats.iOStats.nanos > prevStats.iOStats.nanos && prevStats.iOStats.nanos > 0) {
                this.iOGaugeStats = new IOGaugeStats();
                this.iOGaugeStats.ioBps = (long) ((currentStats.iOStats.ioBytes - prevStats.iOStats.ioBytes) * 1e9 / (currentStats.iOStats.nanos - prevStats.iOStats.nanos));
            }
        }

        public static class CpuGaugeStats {

            private double systemCpuPercent;
            private double userCpuPercent;

            public double getCpuPercent() {
                return systemCpuPercent + userCpuPercent;
            }

        }

        public static class IOGaugeStats {

            public long ioBps;
        }
    }

    private class StatRecord {

        long start;
        long end;
        int running;
        int queded;
        double cpu;
        long rss;
        long swap;
        long io;
    }
}
