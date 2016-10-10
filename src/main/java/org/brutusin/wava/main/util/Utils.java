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
package org.brutusin.wava.main.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.brutusin.commons.Bean;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.json.spi.JsonNode;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.core.Event;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.data.OpName;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class Utils {

    public final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static final Pattern EVENT_PATTERN;
    static {
        StringBuilder sb = new StringBuilder("(");
        Event[] values = Event.values();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append("|");
            }
            sb.append(values[i]);
        }
        sb.append("):(\\d+):(.+)");
        EVENT_PATTERN = Pattern.compile(sb.toString());
    }

    private Utils() {
    }

    public static void showHelp(Options options, String commandLine) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.err);
        formatter.printHelp(pw, Integer.MAX_VALUE, commandLine, null, options, 4, 4, null);
        pw.flush();
    }

    public static FileLock tryLock(File f) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(f, "rws");
        return raf.getChannel().tryLock();
    }

    public static void validateCoreRunning() throws IOException {
        FileLock lock = Utils.tryLock(new File(Environment.ROOT, ".lock"));
        if (lock != null) {
            System.err.println(ANSIColor.RED + "WAVA core process is not running!" + ANSIColor.RESET);
            System.exit(-2);
        }
    }

    public static Integer executeRequest(OpName opName, Object input) throws IOException, InterruptedException {
        if (input == null) {
            return -1;
        }
        File counterFile = new File(Environment.ROOT, "state/.seq");
        long id = Miscellaneous.getGlobalAutoIncremental(counterFile);
        String json = JsonCodec.getInstance().transform(input);
        File requestFile = new File(Environment.ROOT, "request/" + id + "-" + opName);
        File streamRoot = new File(Environment.ROOT, "streams/" + id);
        Miscellaneous.createDirectory(streamRoot);
        final File lifeCycleNamedPipe = new File(streamRoot, "lifecycle");
        final File stdoutNamedPipe = new File(streamRoot, "stdout");
        final File stderrNamedPipe = new File(streamRoot, "stderr");
        LinuxCommands.getInstance().createNamedPipes(lifeCycleNamedPipe, stderrNamedPipe, stdoutNamedPipe);
        final Bean<Integer> retCode = new Bean<>();
        Thread lcThread;
        lcThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream lcIs = new FileInputStream(lifeCycleNamedPipe);
                    BufferedReader br = new BufferedReader(new InputStreamReader(lcIs));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String color = ANSIColor.RESET;
                        String value = line;
                        Matcher matcher = EVENT_PATTERN.matcher(line);
                        if (matcher.matches()) {
                            Event evt = Event.valueOf(matcher.group(1));
                            String date = Utils.DATE_FORMAT.format(new Date(Long.valueOf(matcher.group(2))));
                            String json = matcher.group(3);
                            JsonNode node = JsonCodec.getInstance().parse(json);
                            if (node.getNodeType() == JsonNode.Type.STRING) {
                                value = node.asString();
                            }
                            if (evt == Event.ping) {
                                return;
                            } else if (evt == Event.id || evt == Event.start) {
                                color = ANSIColor.CYAN;
                            } else if (evt == Event.warn) {
                                color = ANSIColor.YELLOW;
                            } else if (evt == Event.error || evt == Event.interrupted) {
                                color = ANSIColor.RED;
                            } else if (evt == Event.retcode) {
                                retCode.setValue(node.asInteger());
                                if (retCode.getValue() == 0) {
                                    color = ANSIColor.GREEN;
                                } else {
                                    color = ANSIColor.RED;
                                }
                            } else if (evt == Event.info) {
                                color = ANSIColor.GREEN;
                            }
                            System.err.println(color + "[" + date + "][scheduler:" + evt + "] " + value + ANSIColor.RESET);
                        }
                    }
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            }
        };
        Thread outThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream outIs = new FileInputStream(stdoutNamedPipe);
                    Miscellaneous.pipeAsynchronously(outIs, System.out);
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            }
        };
        Thread errThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream errIs = new FileInputStream(stderrNamedPipe);
                    BufferedReader br = new BufferedReader(new InputStreamReader(errIs));
                    String line;
                    while ((line = br.readLine()) != null) {
                        System.err.println(line);
                    }
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            }
        };
        lcThread.start();
        outThread.start();
        errThread.start();
        Miscellaneous.writeStringToFile(requestFile, json, "UTF-8");
        lcThread.join();
        outThread.join();
        errThread.join();
        return retCode.getValue();
    }
}
