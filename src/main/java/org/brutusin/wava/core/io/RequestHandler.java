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
package org.brutusin.wava.core.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
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
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.core.Scheduler;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.SubmitInput;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.utils.RetCode;
import org.brutusin.wava.utils.Utils;

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
    private final File streamsFolder;

    public RequestHandler(Scheduler scheduler) throws IOException {
        this.scheduler = scheduler;
        this.requestFolder = new File(Environment.TEMP, "request");
        this.streamsFolder = new File(Environment.TEMP, "streams");
        remakeFolder(requestFolder);
        remakeFolder(streamsFolder);
        remakeFolder(new File(Environment.TEMP, "temp"));
        remakeFolder(new File(Environment.TEMP, "state"));
    }

    private static void remakeFolder(File f) throws IOException {
        Miscellaneous.deleteDirectory(f);
        Miscellaneous.createDirectory(f);
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
                    WatchEvent.Kind<?> kind = watchEvent.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        LOGGER.log(Level.SEVERE, null, "Overflow event retrieved from watch service");
                        continue;
                    }
                    final File file = dir.resolve(((WatchEvent<Path>) watchEvent).context()).toFile();
                    //System.err.println(kind);
                    //System.err.println(file);
                    Thread t = new Thread() {
                        @Override
                        public void run() {
                            try {
                                handleRequest(file);
                            } catch (Throwable th) {
                                Logger.getLogger(RequestHandler.class.getName()).log(Level.SEVERE, null, th);
                            }
                        }
                    };
                    t.start();
                }
                if (!key.reset()) {
                    LOGGER.log(Level.SEVERE, null, "Request directory is inaccessible");
                    break;
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
            String json = new String(Files.readAllBytes(requestFile.toPath()));
            PeerChannel ch = null;
            try {

                if (opName == OpName.submit) {
                    SubmitInput input = JsonCodec.getInstance().parse(json, SubmitInput.class);
                    PeerChannel<SubmitInput> channel = new PeerChannel(user, input, new File(streamsFolder, String.valueOf(id)));
                    ch = channel;
                    this.scheduler.submit(channel);
                } else if (opName == OpName.cancel) {
                    CancelInput input = JsonCodec.getInstance().parse(json, CancelInput.class);
                    PeerChannel<CancelInput> channel = new PeerChannel(user, input, new File(streamsFolder, String.valueOf(id)));
                    ch = channel;
                    this.scheduler.cancel(channel);
                } else if (opName == OpName.jobs) {
                    Boolean noHeaders = JsonCodec.getInstance().parse(json, Boolean.class);
                    PeerChannel<Void> channel = new PeerChannel(user, null, new File(streamsFolder, String.valueOf(id)));
                    ch = channel;
                    this.scheduler.listJobs(channel, noHeaders);
                } else if (opName == OpName.group) {
                    GroupInput input = JsonCodec.getInstance().parse(json, GroupInput.class);
                    if (input.isList()) {
                        PeerChannel<Void> channel = new PeerChannel(user, null, new File(streamsFolder, String.valueOf(id)));
                        ch = channel;
                        this.scheduler.listGroups(channel, input.isNoHeaders());
                    } else {
                        PeerChannel<GroupInput> channel = new PeerChannel(user, input, new File(streamsFolder, String.valueOf(id)));
                        ch = channel;
                        this.scheduler.updateGroup(channel);
                    }
                }
            } catch (Throwable th) {
                if (th instanceof IllegalArgumentException) {
                    PeerChannel.println(ch.getStderrOs(), ANSICode.RED + "[wava] " + th.getMessage());
                    ch.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                } else if (th instanceof InterruptedException) {
                    throw (InterruptedException) th;
                } else if (th instanceof OrphanChannelException) {
                    LOGGER.log(Level.WARNING, "Error processing request " + id + ": Orphan channel found");
                } else {
                    LOGGER.log(Level.SEVERE, "Error processing request " + id + ": " + th.getMessage() + "\noperation:" + opName + "\nuser:" + user + "\njson:" + json, th);
                    PeerChannel.println(ch.getStderrOs(), ANSICode.RED + "[wava] An error has ocurred processing request " + id + ". See core process logs for more details");
                    ch.sendEvent(Event.retcode, RetCode.ERROR.getCode());
                }
                if (ch != null) {
                    ch.close();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println(JsonCodec.getInstance().parse("", SubmitInput.class));
    }

}
