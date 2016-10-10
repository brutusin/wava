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
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.core.ANSIColor;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.core.RequestHandler;
import org.brutusin.wava.core.Scheduler;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class CoreMain {

    private static FileLock tryLock(File f) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(f, "rws");
        return raf.getChannel().tryLock();
    }

    public static void main(String[] args) throws Exception {
        FileLock lock = tryLock(new File(Environment.ROOT, ".lock"));
        if (lock == null) {
            System.err.println(ANSIColor.RED + "Another WAVA core process is running!" + ANSIColor.RESET);
            System.exit(-2);
        }
        try {
            Scheduler scheduler = new Scheduler();
            RequestHandler requestHandler = new RequestHandler(scheduler);
            requestHandler.start();
        } finally {
            lock.release();
        }
    }
}
