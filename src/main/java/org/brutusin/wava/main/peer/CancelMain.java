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

import org.brutusin.wava.main.util.ANSIColor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.FileLock;
import java.util.Date;
import java.util.regex.Matcher;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.brutusin.commons.Bean;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.json.spi.JsonNode;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.core.Event;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.data.CancelInfo;
import org.brutusin.wava.data.OpName;
import org.brutusin.wava.data.SubmitInfo;
import org.brutusin.wava.main.util.Utils;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class CancelMain {

    private static CancelInfo getRequest(String[] args) {
        Options options = new Options();
        Option jOpt = Option.builder("j")
                .argName("job id")
                .hasArg()
                .required()
                .build();
        options.addOption(jOpt);

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args);

            int id;
            try {
                id = Integer.valueOf(cl.getOptionValue("j"));
            } catch (NumberFormatException ex) {
                throw new ParseException("Invalid memory (-j) value");
            }
            CancelInfo ci = new CancelInfo();
            ci.setId(id);
            return ci;
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() + "\n");
            showHelp(options);
            return null;
        }
    }

    private static void showHelp(Options options) {
        Utils.showHelp(options, "wava-cancel.sh [options] [command]\nCancel a running or enqueued job");
    }

    public static void main(String[] args) throws Exception {
        Utils.validateCoreRunning();
        CancelInfo ci = getRequest(args);
        Integer retCode = Utils.executeRequest(OpName.cancel, ci);
        if (retCode == null) {
            retCode = 1;
        }
        System.exit(retCode);
    }
}
