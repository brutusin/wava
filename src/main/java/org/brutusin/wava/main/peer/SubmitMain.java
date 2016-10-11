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

import java.io.File;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.wava.data.OpName;
import org.brutusin.wava.data.SubmitInfo;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class SubmitMain {

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
        Option gOpt = Option.builder("g").argName("group id")
                .hasArg()
                .desc("priority group of the execution. Jobs of the same group follow a FIFO ordering")
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
                String group = cl.getOptionValue("g");
                ri.setGroupName(group);
            }
            return ri;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
            return null;
        }
    }

    private static void showHelp(Options options) {
        Utils.showHelp(options, "wava.sh [options] [command]\nEnqueues a command to be executed [W]hen enough RSS memory is [AVA]ilable");
    }

    public static void main(String[] args) throws Exception {
        Utils.validateCoreRunning();
        SubmitInfo ri = getRequest(args);
        Integer retCode = Utils.executeRequest(OpName.submit, ri);
        if (retCode == null) {
            retCode = 1;
        }
        System.exit(retCode);
    }
}
