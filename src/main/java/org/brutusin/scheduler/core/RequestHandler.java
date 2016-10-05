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
package org.brutusin.scheduler.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.ParseException;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import org.brutusin.scheduler.data.RequestInfo;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(RequestHandler.class.getName());

    private final Scheduler scheduler;

    private final File requestFolder;

    public RequestHandler(Scheduler scheduler) throws IOException {
        this.scheduler = scheduler;
        this.requestFolder = new File(Environment.ROOT, "request");
        if (!requestFolder.exists()) {
            Miscellaneous.createDirectory(requestFolder);
        }
    }

    public void start() throws IOException {
        final WatchService watcher = FileSystems.getDefault().newWatchService();
        Path dir = Paths.get(requestFolder.getAbsolutePath());
        dir.register(watcher, ENTRY_CREATE);
        outer:
        while (true) {
            try {
                if (Thread.interrupted()) {
                    break outer;
                }
                WatchKey key = watcher.take();
                for (WatchEvent<?> watchEvent : key.pollEvents()) {
                    if (Thread.interrupted()) {
                        break outer;
                    }
                    if (watchEvent.kind() == StandardWatchEventKinds.OVERFLOW) {
                        LOGGER.log(Level.SEVERE, null, "Overflow event retrieved from watch service");
                        continue;
                    }
                    handleRequest(dir.resolve(((WatchEvent<Path>) watchEvent).context()).toFile());
                    key.reset();
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Throwable th) {
                LOGGER.log(Level.SEVERE, th.getMessage(), th);
            }
        }
        watcher.close();
    }

    private void handleRequest(File requestFile) throws IOException, InterruptedException, ParseException {
        String name = requestFile.getName();
        int index = name.indexOf("-schedule.json");
        if (index > 0) {
            Integer id = Integer.valueOf(name.substring(0, index));
            String user = LinuxCommands.getInstance().getFileOwner(requestFile);
            FileInputStream fis = new FileInputStream(requestFile);
            String contents = Miscellaneous.toString(fis, "UTF-8");
            RequestInfo ri = JsonCodec.getInstance().parse(contents, RequestInfo.class);
            fis.close();
            requestFile.delete();
            this.scheduler.submit(id, user, ri);
        }
    }

    public static void main(String[] args) throws Exception {
        RequestInfo ri = new RequestInfo();
        ri.setCommand(new String[]{"ls"});
        ri.setWorkingDirectory(new File("/tmp"));
        ri.setMaxRSS(500000);
        System.out.println(JsonCodec.getInstance().transform(ri));

        String s = JsonCodec.getInstance().transform(new File("/tmp"));
        System.out.println(s);
        System.out.println(JsonCodec.getInstance().parse(s, File.class).getAbsolutePath());
        System.out.println(JsonCodec.getInstance().parse(s, File.class).getAbsolutePath());
        System.out.println(ri.getWorkingDirectory().getAbsolutePath());
    }

}
