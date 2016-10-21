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
import org.brutusin.wava.data.CancelInfo;
import org.brutusin.wava.utils.Utils;
import org.brutusin.wava.data.OpName;
import org.brutusin.wava.data.PriorityInfo;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class ChangePriority {

    private static void showHelp(Options options) {
        Utils.showHelp(options, "wava.sh -p [options]\nChanges the priority of a job group");
    }
    
    private static PriorityInfo getRequest(String[] args) {
        Options options = new Options();
        Option gOpt = Option.builder("g")
                .argName("group name")
                .hasArg()
                .required()
                .build();
        Option pOpt = Option.builder("p")
                .argName("integer")
                .desc("new priority")
                .hasArg()
                .required()
                .build();
        
        options.addOption(gOpt);
        options.addOption(pOpt);

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args);

            int priority;
            try {
                priority = Integer.valueOf(cl.getOptionValue(pOpt.getOpt()));
            } catch (NumberFormatException ex) {
                throw new ParseException("Invalid memory (-j) value");
            }
            PriorityInfo pi = new PriorityInfo();
            pi.setGroupName(cl.getOptionValue(gOpt.getOpt()));
            pi.setPriority(priority);
            return pi;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
            return null;
        }
    }
    
    public static void main(String[] args) throws Exception {
        Utils.validateCoreRunning();
        PriorityInfo pi = getRequest(args);
        Integer retCode = Utils.executeRequest(OpName.changePriority, pi, null, false);
        if (retCode == null) {
            retCode = 1;
        }
        System.exit(retCode);
    }
}
