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

import org.brutusin.wava.main.peer.*;
import org.brutusin.wava.utils.Utils;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class WavaMain {

    private static void showHelp(Options options) {
        Utils.showHelp(options, "wava.sh option");
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option hOpt = new Option("h", "print this message");
        Option cOpt = Option.builder("c")
                .longOpt("core")
                .desc("run core wava process")
                .build();
        Option sOpt = Option.builder("s")
                .longOpt("submit")
                .desc("submit command. Default option")
                .build();
        Option jOpt = Option.builder("j")
                .longOpt("jobs")
                .desc("view jobs")
                .build();
        Option pOpt = Option.builder("p")
                .longOpt("priority")
                .desc("change group priority")
                .build();

        options.addOption(hOpt);
        options.addOption(cOpt);
        options.addOption(sOpt);
        options.addOption(jOpt);
        options.addOption(pOpt);
        try {
            if (args.length > 0) {
                CommandLineParser parser = new DefaultParser();
                CommandLine cl = parser.parse(options, Arrays.copyOfRange(args, 0, 1));
                String[] subArgs;
                if (args.length > 1) {
                    subArgs = Arrays.copyOfRange(args, 1, args.length);
                } else {
                    subArgs = new String[0];
                }
                if (cl.hasOption(hOpt.getOpt())) {
                    showHelp(options);
                } else if (cl.hasOption(cOpt.getOpt())) {
                    CoreMain.main(subArgs);
                } else if (cl.hasOption(jOpt.getOpt())) {
                    ListProcessesMain.main(subArgs);
                } else if (cl.hasOption(pOpt.getOpt())) {
                    ChangePriority.main(subArgs);
                } else if (cl.hasOption(sOpt.getOpt())) {
                    SubmitMain.main(subArgs);
                }
            } else {
                showHelp(options);
            }
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
        }
    }
}