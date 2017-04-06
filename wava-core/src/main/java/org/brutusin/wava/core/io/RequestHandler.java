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

import org.brutusin.wava.io.OpName;
import org.brutusin.wava.io.Event;
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
import org.brutusin.wava.env.WavaTemp;
import org.brutusin.wava.core.Scheduler;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.ExtendedSubmitInput;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.io.RetCode;

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
    private Thread mainThread;

    public RequestHandler(Scheduler scheduler) throws IOException {
        this.scheduler = scheduler;
        this.requestFolder = new File(WavaTemp.getInstance().getTemp(), "request");
        this.streamsFolder = new File(WavaTemp.getInstance().getTemp(), "streams");
        remakeFolder(requestFolder);
        remakeFolder(streamsFolder);
        remakeFolder(new File(WavaTemp.getInstance().getTemp(), "temp"));
        remakeFolder(new File(WavaTemp.getInstance().getTemp(), "state"));
    }

    private static void remakeFolder(File f) throws IOException {
        Miscellaneous.deleteDirectory(f);
        Miscellaneous.createDirectory(f);
    }

    public void start() throws IOException {
        mainThread = Thread.currentThread();
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            Path dir = Paths.get(requestFolder.getAbsolutePath());
            dir.register(watcher, ENTRY_CREATE);
            outer:
            while (true) {
                try {
                    if (Thread.interrupted()) {
                        break outer;
                    }
                    while (handleRequests()) {
                    }
                    WatchKey key = watcher.take();
                    key.pollEvents();
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
        }
    }

    private boolean handleRequests() throws IOException, InterruptedException, ParseException {
        File[] listFiles = requestFolder.listFiles();
        boolean ret = false;
        if (listFiles != null) {
            for (int i = 0; i < listFiles.length; i++) {
                File requestFile = listFiles[i];
                Matcher matcher = OP_FILE_PATTERN.matcher(requestFile.getName());
                if (matcher.matches()) {
                    ret = true;
                    final Integer id = Integer.valueOf(matcher.group(1));
                    final OpName opName = OpName.valueOf(matcher.group(2));
                    final String user = LinuxCommands.getInstance().getFileOwner(requestFile);
                    final String json = new String(Files.readAllBytes(requestFile.toPath()));
                    Thread t = new Thread() {
                        @Override
                        public void run() {
                            try {
                                handleRequest(id, opName, user, json);
                            } catch (Throwable th) {
                                Logger.getLogger(RequestHandler.class.getName()).log(Level.SEVERE, null, th);
                            }
                        }
                    };
                    t.start();
                }
                requestFile.delete();
            }
        }
        return ret;
    }

    private void handleRequest(Integer id, OpName opName, String user, String json) throws IOException, InterruptedException {
        PeerChannel ch = null;
        try {

            if (opName == OpName.submit) {
                ExtendedSubmitInput input = JsonCodec.getInstance().parse(json, ExtendedSubmitInput.class);
                PeerChannel<ExtendedSubmitInput> channel = new PeerChannel(user, input, new File(streamsFolder, String.valueOf(id)));
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
            } else if (opName == OpName.exit) {
                String input = JsonCodec.getInstance().parse(json, String.class);
                PeerChannel<String> channel = new PeerChannel(user, input, new File(streamsFolder, String.valueOf(id)));
                if (this.scheduler.close(channel)) {
                    mainThread.interrupt();
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

    public static void main(String[] args) throws Exception {

        System.out.println(JsonCodec.getInstance().parse("", ExtendedSubmitInput.class));
    }

}
