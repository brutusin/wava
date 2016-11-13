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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;
import org.brutusin.commons.Bean;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.json.spi.JsonNode;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.core.plug.LinuxCommands;
import org.brutusin.wava.utils.ANSICode;
import org.brutusin.wava.utils.Utils;
import static org.brutusin.wava.utils.Utils.parseEventLine;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class RequestExecutor {

    public static Integer executeRequest(OpName opName, Object input, final OutputStream eventStream, boolean prettyEvents) throws IOException, InterruptedException {
        File counterFile = new File(Environment.TEMP, "state/.seq");
        long id = Miscellaneous.getGlobalAutoIncremental(counterFile);
        String json = JsonCodec.getInstance().transform(input);
        File streamRoot = new File(Environment.TEMP, "streams/" + id);
        Miscellaneous.createDirectory(streamRoot);
        File eventsNamedPipe = new File(streamRoot, PeerChannel.NamedPipe.events.name());
        File stdoutNamedPipe = new File(streamRoot, PeerChannel.NamedPipe.stdout.name());
        File stderrNamedPipe = new File(streamRoot, PeerChannel.NamedPipe.stderr.name());
        LinuxCommands.getInstance().createNamedPipes(eventsNamedPipe, stderrNamedPipe, stdoutNamedPipe);
        final Bean<Integer> retCode = new Bean<>();
        Thread eventsThread;
        eventsThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream eventsIs = new FileInputStream(eventsNamedPipe);
                    BufferedReader br = new BufferedReader(new InputStreamReader(eventsIs));
                    String line;
                    while ((line = br.readLine()) != null) {
                        List<String> tokens = parseEventLine(line);
                        Event evt = Event.valueOf(tokens.get(1));
                        String value;
                        if (tokens.size() > 2) {
                            value = tokens.get(2);
                        } else {
                            value = null;
                        }

                        if (evt == Event.ping) {

                        } else {
                            if (evt == Event.retcode) {
                                retCode.setValue(Integer.valueOf(value));
                            }
                            if (eventStream != null) {
                                if (!prettyEvents) {
                                    synchronized (eventStream) {
                                        eventStream.write((line + "\n").getBytes());
                                    }
                                } else {
                                    Date date = new Date(Long.valueOf(tokens.get(0)));
                                    ANSICode color = ANSICode.CYAN;
                                    if (evt == Event.id || evt == Event.running) {
                                        color = ANSICode.GREEN;
                                    } else if (evt == Event.queued) {
                                        color = ANSICode.YELLOW;
                                    } else if (evt == Event.cancelled) {
                                        color = ANSICode.YELLOW;
                                    } else if (evt == Event.retcode) {
                                        if (retCode.getValue() == 0) {
                                            color = ANSICode.GREEN;
                                        } else {
                                            color = ANSICode.RED;
                                        }
                                    } else if (evt == Event.exceed_allowed) {
                                        color = ANSICode.YELLOW;
                                    } else if (evt == Event.exceed_disallowed) {
                                        color = ANSICode.RED;
                                    } else if (evt == Event.exceed_global) {
                                        color = ANSICode.YELLOW;
                                    } else if (evt == Event.error) {
                                        color = ANSICode.RED;
                                        if (value != null) {
                                            JsonNode node = JsonCodec.getInstance().parse(value);
                                            value = node.asString();
                                        }
                                    }
                                    synchronized (eventStream) {
                                        eventStream.write((color.getCode() + "[wava] [" + Utils.DATE_FORMAT.format(date) + "] [" + evt + (value != null ? (":" + value) : "") + "]" + ANSICode.RESET.getCode() + "\n").getBytes());
                                    }
                                }
                            }
                        }
                    }
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                }
            }
        };
        Thread outThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream outIs = new FileInputStream(stdoutNamedPipe);
                    Miscellaneous.pipeAsynchronously(outIs, System.out);
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                }
            }
        };
        Thread errThread = new Thread() {
            @Override
            public void run() {
                InputStream errIs = null;
                try {
                    errIs = new FileInputStream(stderrNamedPipe);
                    BufferedReader br = new BufferedReader(new InputStreamReader(errIs));
                    Miscellaneous.pipeSynchronously(br, false, System.err);
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                } finally {
                    if (errIs != null) {
                        try {
                            errIs.close();
                        } catch (IOException ex) {
                            ex.printStackTrace(System.err);
                        }
                    }
                }
            }
        };
        eventsThread.start();
        outThread.start();
        errThread.start();
        File tempFile = new File(Environment.TEMP, "temp/" + id + "-" + opName);
        File requestFile = new File(Environment.TEMP, "request/" + id + "-" + opName);
        Files.write(tempFile.toPath(), json.getBytes());
        Files.move(tempFile.toPath(), requestFile.toPath());
        eventsThread.join();
        outThread.join();
        errThread.join();
        return retCode.getValue();
    }

}
