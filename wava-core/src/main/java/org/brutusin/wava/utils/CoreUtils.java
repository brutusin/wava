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

import org.brutusin.wava.io.RetCode;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.env.WavaTemp;
import org.brutusin.wava.Utils;
import org.brutusin.wava.WavaNotRunningException;
import org.brutusin.wava.main.WavaMain;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class CoreUtils {

    private CoreUtils() {
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

    public static void validateCoreRunning() throws IOException, WavaNotRunningException {
        if (!Utils.isCoreRunning()) {
           throw new WavaNotRunningException();
        }
    }

    public static void main(String[] args) {
        System.out.println(("[W]hen [AVA]ilable scheduler ").length());
    }
}
