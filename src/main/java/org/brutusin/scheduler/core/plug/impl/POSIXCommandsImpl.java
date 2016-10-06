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
package org.brutusin.scheduler.core.plug.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.commons.utils.ProcessUtils;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import org.brutusin.scheduler.data.Stats;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class POSIXCommandsImpl extends LinuxCommands {

    public long getSystemRSSUsedMemory() throws IOException, InterruptedException {
        return getSystemRSSMemory() - getSystemRSSFreeMemory();
    }

    public Map<String, Stats> getStats(String[] pids) throws IOException, InterruptedException {
        Map<String, Stats> ret = new HashMap<String, Stats>();
        String[] cmd = {"ps", "-p " + Arrays.toString(pids) + " -o pid,rss,pcpu --no-headers"};
        Process p = Runtime.getRuntime().exec(cmd);
        String stdout = ProcessUtils.execute(p)[0];
        if (stdout != null) {
            String[] lines = stdout.split("\n");
            for (int i = 0; i < lines.length; i++) {
                String[] cols = lines[i].split("\\s+");
                Stats st = new Stats();
                st.setRssBytes(Integer.valueOf(cols[1]));
                st.setCpuPercent(Double.valueOf(cols[2]));
                ret.put(cols[0], st);
            }
        }
        return ret;
    }

    public long getSystemRSSFreeMemory() throws IOException, InterruptedException {
        String ouput = executeBashCommand("echo $((`cat /proc/meminfo | grep ^Cached:| awk '{print $2}'` + `cat /proc/meminfo | grep ^MemFree:| awk '{print $2}'` ))");
        return Long.valueOf(ouput);
    }

    public long getSystemRSSMemory() throws IOException, InterruptedException {
        String ouput = executeBashCommand("cat /proc/meminfo | grep ^MemTotal:| awk '{print $2}'");
        return Long.valueOf(ouput);
    }

    public String[] getRunAsCommand(String user, String[] cmd) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cmd.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(cmd[i]);
        }
        return new String[]{"runuser", "-p", user, "-c", sb.toString()};
    }

    public String getRunningUser() throws IOException, InterruptedException {
        String[] cmd = {"id", "-un"};
        Process p = Runtime.getRuntime().exec(cmd);
        return ProcessUtils.execute(p)[0];
    }

    public void createNamedPipes(File... files) throws IOException, InterruptedException {
        String[] command = new String[files.length + 1];
        command[0] = "mkfifo";
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (!f.getParentFile().exists()) {
                Miscellaneous.createDirectory(f.getParentFile());
            }
            command[i + 1] = f.getAbsolutePath();
        }
        Process p = Runtime.getRuntime().exec(command);
        ProcessUtils.execute(p);
    }

    public String getFileOwner(File f) throws IOException, InterruptedException {
        return executeBashCommand("ls -ld \"" + f.getAbsolutePath() + "\" | awk 'NR==1 {print $3}'");
    }

    private static String executeBashCommand(String command) throws IOException, InterruptedException {
        String[] cmd = {"/bin/bash", "-c", command};
        Process p = Runtime.getRuntime().exec(cmd);
        String[] ret = ProcessUtils.execute(p);
        return ret[0];
    }
}
