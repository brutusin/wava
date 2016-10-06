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
package org.brutusin.scheduler.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.scheduler.core.Environment;
import org.brutusin.scheduler.core.Event;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import org.brutusin.scheduler.data.RequestInfo;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class PeerProcess {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

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
        sb.append("):(.*)");
        EVENT_PATTERN = Pattern.compile(sb.toString());
    }

    private static RequestInfo getRequest(String[] args) {
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

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args);
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
            RequestInfo ri = new RequestInfo();
            ri.setCommand(cl.getArgs());
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
        formatter.printHelp(pw, Integer.MAX_VALUE, "schedule.sh [options] [command]\nEnqueues a command to be executed when enough RSS memory is available", "\nOptions:", options, 4, 4, "");
        pw.flush();
    }

    public static void main(String[] args) throws Exception {
        System.out.println(EVENT_PATTERN);
        File counterFile = new File(Environment.ROOT, "state/.seq");
        RequestInfo ri = getRequest(args);
        if (ri != null) {
            long id = Miscellaneous.getGlobalAutoIncremental(counterFile);
            String json = JsonCodec.getInstance().transform(ri);
            File requestFile = new File(Environment.ROOT, "request/" + id + "-schedule.json");
            File streamRoot = new File(Environment.ROOT, "streams/" + id);
            Miscellaneous.createDirectory(streamRoot);
            final File lifeCycleNamedPipe = new File(streamRoot, "lifecycle");
            final File stdoutNamedPipe = new File(streamRoot, "stdout");
            final File stderrNamedPipe = new File(streamRoot, "stderr");
            LinuxCommands.getInstance().createNamedPipes(lifeCycleNamedPipe, stderrNamedPipe, stdoutNamedPipe);

            Thread lcThread = new Thread() {
                @Override
                public void run() {
                    try {
                        InputStream lcIs = new FileInputStream(lifeCycleNamedPipe);
                        BufferedReader br = new BufferedReader(new InputStreamReader(lcIs));
                        String line;
                        while ((line = br.readLine()) != null) {
                            Matcher matcher = EVENT_PATTERN.matcher(line);
                            Event evt = Event.valueOf(matcher.group(1));
                            String color = ANSI_RESET;
                            if (evt == Event.warn) {
                                color = ANSI_YELLOW;
                            } else if (evt == Event.error || evt == Event.interrupted) {
                                color = ANSI_RED;
                            } else if (evt == Event.start || evt == Event.retcode) {
                                color = ANSI_BLUE;
                            } else if (evt == Event.info) {
                                color = ANSI_GREEN;
                            }
                            System.err.println(color + line + ANSI_RESET);
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
        }
    }
}
