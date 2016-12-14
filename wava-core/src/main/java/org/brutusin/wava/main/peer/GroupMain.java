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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.wava.core.io.CommandLineRequestExecutor;
import org.brutusin.wava.utils.CoreUtils;
import org.brutusin.wava.io.OpName;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.io.RetCode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class GroupMain {

    public static final String DESCRIPTION = "group management commands";

    private static void showHelp(Options options) {
        CoreUtils.showHelp(options, "wava -g [options]\n" + DESCRIPTION);
    }

    private static GroupInput getRequest(String[] args) {
        Options options = new Options();
        Option nOpt = Option.builder("n")
                .longOpt("name")
                .argName("group name")
                .desc("name of the group to be created or updated.")
                .hasArg()
                .build();
        Option dOpt = Option.builder("d")
                .longOpt("delete")
                .desc("deletes an existing empty group")
                .build();
        Option lOpt = Option.builder("l")
                .longOpt("list")
                .desc("list existing groups")
                .build();
        Option hOpt = Option.builder("h")
                .longOpt("no-headers")
                .desc("do not output headers")
                .build();
        Option pOpt = Option.builder("p")
                .longOpt("priority")
                .argName("integer")
                .desc("priority")
                .hasArg()
                .build();
        Option tOpt = Option.builder("t")
                .longOpt("idle")
                .argName("integer")
                .desc("time to idle. Elapsed time since the last executing job of the group finishes and the group is deleted. Default is -1, meaning that the group to be created is eternal")
                .hasArg()
                .build();

        options.addOption(dOpt);
        options.addOption(nOpt);
        options.addOption(pOpt);
        options.addOption(tOpt);
        options.addOption(lOpt);
        options.addOption(hOpt);

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args);
            GroupInput gi = new GroupInput();
            if (cl.hasOption(nOpt.getOpt())) {
                gi.setGroupName(cl.getOptionValue(nOpt.getOpt()));
                if (cl.hasOption(dOpt.getOpt())) {
                    gi.setDelete(true);
                } else {
                    if (cl.hasOption(pOpt.getOpt())) {
                        try {
                            gi.setPriority(Integer.valueOf(cl.getOptionValue(pOpt.getOpt())));
                        } catch (NumberFormatException ex) {
                            throw new ParseException("Invalid " + pOpt.getOpt() + " value");
                        }
                    }
                    if (cl.hasOption(tOpt.getOpt())) {
                        try {
                            gi.setTimetoIdleSeconds(Integer.valueOf(cl.getOptionValue(tOpt.getOpt())));
                        } catch (NumberFormatException ex) {
                            throw new ParseException("Invalid " + tOpt.getOpt() + " value");
                        }
                    }
                }
            } else if (cl.hasOption(lOpt.getOpt())) {
                gi.setList(true);
                if (cl.hasOption(hOpt.getOpt())) {
                    gi.setNoHeaders(true);
                }
            } else {
                showHelp(options);
                return null;
            }
            return gi;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        CoreUtils.validateCoreRunning();
        GroupInput gi = getRequest(args);
        if (gi == null) {
            System.exit(RetCode.ERROR.getCode());
        }
        System.exit(new CommandLineRequestExecutor().executeRequest(OpName.group, gi));
    }
}
