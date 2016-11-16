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
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.main.WavaMain;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class Utils {

    public final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    private Utils() {
    }

    public static List<String> parseEventLine(String line) {
        List<String> ret = new ArrayList<>();
        int start = 0;
        boolean inString = false;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '"' && (i == 0 || line.charAt(i - 1) != '\\')) {
                inString = !inString;
            }
            if (!inString && line.charAt(i) == ':') {
                ret.add(line.substring(start, i));
                start = i + 1;
            }
        }
        if (start < line.length()) {
            ret.add(line.substring(start));
        }
        return ret;
    }

    public static void showHelp(Options options, String commandLine) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.err);
        System.err.println(getLogo());
        formatter.printHelp(pw, Integer.MAX_VALUE, commandLine, null, options, 4, 4, null);
        pw.flush();
    }

    public static String getVersion() {
        try {
            return Miscellaneous.toString(WavaMain.class.getClassLoader().getResourceAsStream("version.txt"), "UTF-8");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String getBuildDate() {
        try {
            return Miscellaneous.toString(WavaMain.class.getClassLoader().getResourceAsStream("build-date.txt"), "UTF-8");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String getLogo() {
        try {
            String version = getVersion();
            String line = Miscellaneous.append("_", 29 + version.length());
            StringBuilder sb = new StringBuilder(line);
            sb.append("\n");
            sb.append(ANSICode.BOLD.getCode());
            sb.append(ANSICode.GREEN.getCode());
            sb.append(Miscellaneous.toString(WavaMain.class.getClassLoader().getResourceAsStream("logo.txt"), "UTF-8"));
            sb.append(ANSICode.RESET.getCode());
            sb.append("\n");
            sb.append("\n");
            sb.append(ANSICode.GREEN.getCode());
            sb.append("[W]");
            sb.append(ANSICode.CYAN.getCode());
            sb.append("hen ");
            sb.append(ANSICode.GREEN.getCode());
            sb.append("[AVA]");
            sb.append(ANSICode.CYAN.getCode());
            sb.append("ilable scheduler ");
            sb.append(ANSICode.RED.getCode());
            sb.append(version);
            sb.append(ANSICode.RESET.getCode());
            sb.append("\n");
            sb.append(line);
            return sb.toString();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static FileLock tryLock(File f) throws IOException {
        if (!f.exists()) {
            Miscellaneous.createFile(f.getAbsolutePath());
        }
        RandomAccessFile raf = new RandomAccessFile(f, "rws");
        return raf.getChannel().tryLock();
    }

    public static void validateCoreRunning() throws IOException {
        FileLock lock = Utils.tryLock(new File(Environment.TEMP, ".lock"));
        if (lock != null) {
            System.err.println(ANSICode.RED.getCode() + "WAVA core process is not running!" + ANSICode.RESET.getCode());
            System.exit(RetCode.CORE_NOT_RUNNING.getCode());
        }
    }

    public static void main(String[] args) {
        System.out.println(("[W]hen [AVA]ilable scheduler ").length());
    }
}
