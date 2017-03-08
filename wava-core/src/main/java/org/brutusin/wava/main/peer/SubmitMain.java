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
package org.brutusin.wava.main.peer;

import org.brutusin.wava.utils.CoreUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.commons.Pair;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.core.io.CommandLineRequestExecutor;
import org.brutusin.wava.env.EnvEntry;
import org.brutusin.wava.input.ExtendedSubmitInput;
import org.brutusin.wava.io.OpName;
import org.brutusin.wava.input.SubmitInput;
import org.brutusin.wava.io.RetCode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class SubmitMain {

    public static final String DESCRIPTION = "enqueue a job to be executed when enough physical memory is available";

    private static void showHelp(Options options) {
        CoreUtils.showHelp(options, "wava -r [options] [command]\n" + DESCRIPTION);
    }

    private static int getCommandStart(Options options, String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            Option option = options.getOption(arg);
            if (option == null) {
                return i;
            }
            if (option.hasArg()) {
                i++;
            }
        }
        return -1;
    }

    private static Pair<SubmitInput, File> getRequest(String[] args) {
        Options options = new Options();
        Option hOpt = Option.builder("h")
                .longOpt("help")
                .desc("print this message")
                .build();
        Option mOpt = Option.builder("m")
                .longOpt("memory")
                .argName("mem value")
                .hasArg()
                .desc("required RSS memory")
                .required()
                .build();
        Option eOpt = Option.builder("e")
                .longOpt("event-file")
                .argName("file")
                .hasArg()
                .desc("file to send execution events. If null, then stderr will be used")
                .build();
        Option gOpt = Option.builder("g")
                .longOpt("group")
                .argName("group id")
                .hasArg()
                .desc("priority group of the execution. Jobs of the same group follow a FIFO ordering")
                .build();
        Option iOpt = Option.builder("i")
                .longOpt("idempotent")
                .desc("priority group of the execution. Jobs of the same group follow a FIFO ordering")
                .build();

        options.addOption(hOpt);
        options.addOption(mOpt);
        options.addOption(gOpt);
        options.addOption(eOpt);
        options.addOption(iOpt);

        int commandStart = getCommandStart(options, args);
        if (commandStart == -1) {
            System.err.println("A command is required");
            showHelp(options);
            return null;
        }
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, Arrays.copyOfRange(args, 0, commandStart));
            if (cl.hasOption(hOpt.getOpt())) {
                showHelp(options);
                return null;
            }
            File eventFile;
            if (cl.hasOption(eOpt.getOpt())) {
                eventFile = new File(cl.getOptionValue(eOpt.getOpt()));
            } else {
                eventFile = null;
            }

            long memory;
            try {
                memory = Miscellaneous.parseHumanReadableByteCount(cl.getOptionValue(mOpt.getOpt()));
            } catch (IllegalArgumentException ex) {
                throw new ParseException("Invalid memory (-" + mOpt.getOpt() + ") value");
            }
            ExtendedSubmitInput ri = new ExtendedSubmitInput();
            String envJobId = System.getenv(EnvEntry.WAVA_JOB_ID.name());
            if (envJobId != null) {
                ri.setParentId(Integer.valueOf(envJobId));
            }
            ri.setCommand(Arrays.copyOfRange(args, commandStart, args.length));
            ri.setMaxRSS(memory);
            ri.setWorkingDirectory(new File(""));
            ri.setEnvironment(System.getenv());
            if (cl.hasOption(gOpt.getOpt())) {
                String group = cl.getOptionValue(gOpt.getOpt());
                ri.setGroupName(group);
            }
            if (cl.hasOption(iOpt.getOpt())) {
                ri.setIdempotent(true);
            }
            return new Pair(ri, eventFile);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        CoreUtils.validateCoreRunning();
        Pair<SubmitInput, File> pair = getRequest(args);
        if (pair == null) {
            System.exit(RetCode.ERROR.getCode());
        }
        OutputStream eventOs;
        boolean prettyEvents;
        if (pair.getElement2() == null) {
            eventOs = System.err;
            prettyEvents = true;
        } else {
            eventOs = new FileOutputStream(pair.getElement2());
            prettyEvents = false;
        }
        Integer retCode = new CommandLineRequestExecutor().executeRequest(OpName.submit, pair.getElement1(), eventOs, prettyEvents);
        if (retCode == null) {
            retCode = RetCode.ERROR.getCode();
        }
        System.exit(retCode);
    }
}
