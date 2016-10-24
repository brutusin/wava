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
package org.brutusin.wava.core;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.ParseException;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.SubmitInput;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class RequestHandler {

    private static final Logger LOGGER = Logger.getLogger(RequestHandler.class.getName());
    public static final Pattern OP_FILE_PATTERN;

    static {
        StringBuilder sb = new StringBuilder("(\\d+)-(");
        OpName[] values = OpName.values();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append("|");
            }
            sb.append(values[i]);
        }
        sb.append(")");
        OP_FILE_PATTERN = Pattern.compile(sb.toString());
    }

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
        Matcher matcher = OP_FILE_PATTERN.matcher(requestFile.getName());
        if (matcher.matches()) {
            Integer id = Integer.valueOf(matcher.group(1));
            OpName opName = OpName.valueOf(matcher.group(2));
            String user = LinuxCommands.getInstance().getFileOwner(requestFile);
            String json;
            try (FileInputStream fis = new FileInputStream(requestFile)) {
                json = Miscellaneous.toString(fis, "UTF-8");
            }
            requestFile.delete();
            if (opName == OpName.submit) {
                SubmitInput input = JsonCodec.getInstance().parse(json, SubmitInput.class);
                PeerChannel<SubmitInput> channel = new PeerChannel(user, input, new File(Environment.ROOT, "/streams/" + id));
                this.scheduler.submit(channel);
            } else if (opName == OpName.cancel) {
                CancelInput input = JsonCodec.getInstance().parse(json, CancelInput.class);
                PeerChannel<CancelInput> channel = new PeerChannel(user, input, new File(Environment.ROOT, "/streams/" + id));
                this.scheduler.cancel(channel);
            } else if (opName == OpName.jobs) {
                PeerChannel<Void> channel = new PeerChannel(user, null, new File(Environment.ROOT, "/streams/" + id));
                this.scheduler.listJobs(channel);
            } else if (opName == OpName.group) {
                GroupInput input = JsonCodec.getInstance().parse(json, GroupInput.class);
                if (input.isList()) {
                    PeerChannel<Void> channel = new PeerChannel(user, null, new File(Environment.ROOT, "/streams/" + id));
                    this.scheduler.listGroups(channel);
                } else {
                    PeerChannel<GroupInput> channel = new PeerChannel(user, input, new File(Environment.ROOT, "/streams/" + id));
                    this.scheduler.updateGroup(channel);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SubmitInput ri = new SubmitInput();
        ri.setCommand(new String[]{"ls"});
        ri.setWorkingDirectory(new File("/tmp"));
        ri.setMaxRSS(500000);
        System.out.println(JsonCodec.getInstance().transform(ri));

        String s = JsonCodec.getInstance().transform(new File("/tmp"));
        System.out.println(s);
        System.out.println(JsonCodec.getInstance().parse(s, File.class
        ).getAbsolutePath());
        System.out.println(JsonCodec.getInstance().parse(s, File.class
        ).getAbsolutePath());
        System.out.println(ri.getWorkingDirectory().getAbsolutePath());
    }

}
