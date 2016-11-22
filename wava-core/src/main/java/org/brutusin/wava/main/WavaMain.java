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

import java.io.File;
import org.brutusin.wava.main.peer.*;
import org.brutusin.wava.utils.CoreUtils;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.wava.env.WavaTemp;
import org.brutusin.wava.env.WavaHomeNotSetError;
import org.brutusin.wava.WavaNotRunningException;
import org.brutusin.wava.env.EnvEntry;
import org.brutusin.wava.env.WavaHome;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.utils.NonRootUserException;
import org.brutusin.wava.io.RetCode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class WavaMain {

    private static void showHelp(Options options) {
        CoreUtils.showHelp(options, "wava [option]");
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option hOpt = Option.builder("h")
                .longOpt("help")
                .desc("print this message")
                .build();
        Option aOpt = Option.builder("a")
                .longOpt("about")
                .desc("information about the program")
                .build();
        Option sOpt = Option.builder("s")
                .longOpt("start")
                .desc("start core scheduler process")
                .build();
        Option uOpt = Option.builder("u")
                .longOpt("update")
                .desc("update to lastest version")
                .build();
        Option rOpt = Option.builder("r")
                .longOpt("run")
                .desc(SubmitMain.DESCRIPTION)
                .build();
        Option jOpt = Option.builder("j")
                .longOpt("jobs")
                .desc("view jobs")
                .build();
        Option gOpt = Option.builder("g")
                .longOpt("group")
                .desc("group management commands")
                .build();
        Option cOpt = Option.builder("c")
                .longOpt("cancel")
                .desc(CancelMain.DESCRIPTION)
                .build();
        Option tOpt = Option.builder("t")
                .longOpt("status")
                .desc(StatusMain.DESCRIPTION)
                .build();
        Option xOpt = Option.builder("x")
                .longOpt("exit")
                .desc("stop core process, terminating all jobs")
                .build();

        options.addOption(aOpt);
        options.addOption(hOpt);
        options.addOption(sOpt);
        options.addOption(rOpt);
        options.addOption(gOpt);
        options.addOption(jOpt);
        options.addOption(cOpt);
        options.addOption(uOpt);
        options.addOption(tOpt);
        options.addOption(xOpt);
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
                if (cl.hasOption(aOpt.getOpt())) {
                    AboutMain.main(subArgs);
                } else if (cl.hasOption(hOpt.getOpt())) {
                    showHelp(options);
                } else if (cl.hasOption(sOpt.getOpt())) {
                    CoreMain.main(subArgs);
                } else if (cl.hasOption(jOpt.getOpt())) {
                    ListJobsMain.main(subArgs);
                } else if (cl.hasOption(gOpt.getOpt())) {
                    GroupMain.main(subArgs);
                } else if (cl.hasOption(rOpt.getOpt())) {
                    SubmitMain.main(subArgs);
                } else if (cl.hasOption(cOpt.getOpt())) {
                    CancelMain.main(args);
                } else if (cl.hasOption(uOpt.getOpt())) {
                    System.err.println("run the following script for updating: " + ANSICode.CYAN + new File(WavaHome.getInstance().getFile(), "bin/wava-update").getAbsolutePath() + ANSICode.RESET);
                } else if (cl.hasOption(tOpt.getOpt())) {
                    StatusMain.main(args);
                } else if (cl.hasOption(xOpt.getOpt())) {
                    ExitMain.main(args);
                }
            } else {
                showHelp(options);
            }
        } catch (ParseException exp) {
            System.err.println(ANSICode.RED + "Parsing failed.  Reason: " + exp.getMessage() + ANSICode.RESET + "\n");
            showHelp(options);
            System.exit(RetCode.ERROR.getCode());
        } catch (NonRootUserException ex) {
            System.err.println(ANSICode.RED + "Only 'root' user can run this command" + ANSICode.RESET);
            System.exit(RetCode.NOT_ROOT_USER.getCode());
        } catch (WavaNotRunningException ex) {
            System.err.println(ANSICode.RED.getCode() + "WAVA core process is not running!" + ANSICode.RESET.getCode());
            System.exit(RetCode.CORE_NOT_RUNNING.getCode());
        } catch (WavaHomeNotSetError th) {
            System.err.println(EnvEntry.WAVA_HOME.name() + " environment variable not set");
            System.exit(RetCode.NOT_WAVA_HOME.getCode());
        } catch (Throwable th) {
            System.err.println(ANSICode.RED + "Severe error: " + th.getMessage() + ANSICode.RESET);
            System.exit(RetCode.ERROR.getCode());
        }
    }
}
