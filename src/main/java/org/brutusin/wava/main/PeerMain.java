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
package org.brutusin.wava.main;

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
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.commons.Bean;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.json.spi.JsonNode;
import org.brutusin.wava.core.ANSIColor;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.core.Event;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.data.SubmitInfo;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class PeerMain {

    private final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final Pattern EVENT_PATTERN;

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

    private static int getCommandStart(String[] args) {
        for (int i = 0; i < args.length; i = i + 2) {
            if (!args[i].startsWith("-")) {
                return i;
            }
        }
        return args.length;
    }

    private static SubmitInfo getRequest(String[] args) {
        Options options = new Options();
        Option hOpt = new Option("h", "print this message");
        Option mOpt = Option.builder("m").argName("bytes number")
                .hasArg()
                .desc("maximum RSS memory to be demanded by the process")
                .required()
                .build();
        Option gOpt = Option.builder("g").argName("positive integer")
                .hasArg()
                .desc("group id of the execution. Group priority can be changed. Jobs of the same group follow a FIFO ordering")
                .build();
        options.addOption(hOpt);
        options.addOption(mOpt);
        options.addOption(gOpt);

        int commandStart = getCommandStart(args);

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, Arrays.copyOfRange(args, 0, commandStart));
            if (commandStart == args.length) {
                System.err.println("A command is required");
                showHelp(options);
                return null;
            }

            if (cl.hasOption("h")) {
                showHelp(options);
                return null;
            }
            long memory;
            try {
                memory = Long.valueOf(cl.getOptionValue("m"));
            } catch (NumberFormatException ex) {
                throw new ParseException("Invalid memory (-m) value");
            }
            SubmitInfo ri = new SubmitInfo();
            ri.setCommand(Arrays.copyOfRange(args, commandStart, args.length));
            ri.setMaxRSS(memory);
            ri.setWorkingDirectory(new File(""));
            ri.setEnvironment(System.getenv());
            if (cl.hasOption("g")) {
                try {
                    int groupId = Integer.valueOf(cl.getOptionValue("g"));
                    ri.setGroupId(groupId);
                } catch (NumberFormatException ex) {
                    throw new ParseException("Invalid memory (-g) value");
                }
            }
            return ri;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
            return null;
        }
    }

    private static void showHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(System.err);
        formatter.printHelp(pw, Integer.MAX_VALUE, "wava.sh [options] [command]\nEnqueues a command to be executed [W]hen enough RSS memory is [AVA]ilable", "\nOptions:", options, 4, 4, "");
        pw.flush();
    }

    private static FileLock tryLock(File f) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(f, "rws");
        return raf.getChannel().tryLock();
    }

    public static void main(String[] args) throws Exception {
        FileLock lock = tryLock(new File(Environment.ROOT, ".lock"));
        if (lock != null) {
            System.err.println(ANSIColor.RED + "WAVA core process is not running!" + ANSIColor.RESET);
            System.exit(-2);
        }
        SubmitInfo ri = getRequest(args);
        if (ri != null) {
            File counterFile = new File(Environment.ROOT, "state/.seq");
            long id = Miscellaneous.getGlobalAutoIncremental(counterFile);
            String json = JsonCodec.getInstance().transform(ri);
            File requestFile = new File(Environment.ROOT, "request/" + id + "-schedule.json");
            File streamRoot = new File(Environment.ROOT, "streams/" + id);
            Miscellaneous.createDirectory(streamRoot);
            final File lifeCycleNamedPipe = new File(streamRoot, "lifecycle");
            final File stdoutNamedPipe = new File(streamRoot, "stdout");
            final File stderrNamedPipe = new File(streamRoot, "stderr");
            LinuxCommands.getInstance().createNamedPipes(lifeCycleNamedPipe, stderrNamedPipe, stdoutNamedPipe);
            final Bean<Integer> retCode = new Bean<>();
            retCode.setValue(-1);
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
                                String date = DATE_FORMAT.format(new Date(Long.valueOf(matcher.group(2))));
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
            System.exit(retCode.getValue());
        }
    }
}
