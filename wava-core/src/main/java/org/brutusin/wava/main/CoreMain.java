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
import java.io.IOException;
import org.brutusin.wava.utils.ANSICode;
import java.nio.channels.FileLock;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.Utils;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.core.io.RequestHandler;
import org.brutusin.wava.core.Scheduler;
import org.brutusin.wava.io.RetCode;
import org.brutusin.wava.utils.CoreUtils;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class CoreMain {

    public static void main(String[] args) throws Exception {
        FileLock lock = Utils.tryWavaLock();
        if (lock == null) {
            System.err.println(ANSICode.RED.getCode() + "Another WAVA core process is running!" + ANSICode.RESET.getCode());
            System.exit(RetCode.ERROR.getCode());
        }
        try {
            configureLogging();
            Scheduler scheduler = new Scheduler();
            RequestHandler requestHandler = new RequestHandler(scheduler);
            scheduler.start();
            requestHandler.start();
        } finally {
            lock.release();
        }
    }

    private static void configureLogging() throws IOException {
        Logger logger = Logger.getLogger("");
        logger.setUseParentHandlers(false);
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (int i = 0; i < handlers.length; i++) {
                Handler handler = handlers[i];
                logger.removeHandler(handler);
            }
        }
        Level level = Level.parse(Config.getInstance().getSchedulerCfg().getLoggingLevel());
        logger.setLevel(level);
        File f = new File(Config.getInstance().getSchedulerCfg().getLogFolder(), "logs");
        if (f.exists()) {
            Miscellaneous.deleteDirectory(f);
        }
        Miscellaneous.createDirectory(f);
        int maxFiles = 10;
        int maxBytesPerFile = (int) (Config.getInstance().getSchedulerCfg().getMaxLogSize() / maxFiles);
        FileHandler fh = new FileHandler(f.getAbsolutePath() + "/wava%g.log", maxBytesPerFile, maxFiles, false);
        fh.setLevel(Level.ALL);
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tY/%1$tm/%1$td %1$tH:%1$tM:%1$tS] [%4$-6s] [%2$s] %5$s%6$s%n");
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.addHandler(fh);
        System.err.println(ANSICode.GREEN.getCode() + "Logging to " + f.getAbsolutePath() + " ..." + ANSICode.RESET.getCode());
        logger.log(Level.SEVERE, "\n" + CoreUtils.getLogo());
    }
}
