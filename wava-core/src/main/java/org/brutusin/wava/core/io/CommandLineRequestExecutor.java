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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.json.ParseException;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.json.spi.JsonNode;
import org.brutusin.wava.Utils;
import org.brutusin.wava.env.EnvEntry;
import org.brutusin.wava.input.Input;
import org.brutusin.wava.io.EventListener;
import org.brutusin.wava.io.LineListener;
import org.brutusin.wava.io.RequestExecutor;
import org.brutusin.wava.utils.ANSICode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class CommandLineRequestExecutor extends RequestExecutor {

    public Integer executeRequest(OpName opName, Input input) throws IOException, InterruptedException {
        return executeRequest(opName, input, null, false);
    }

    public Integer executeRequest(OpName opName, Input input, final OutputStream eventStream, boolean prettyEvents) throws IOException, InterruptedException {

        EventListener evtListener = null;
        if (eventStream != null) {
            evtListener = new EventListener() {
                @Override
                public void onEvent(Event evt, String value, long time) {
                    if (evt == Event.ping) {
                        return;
                    }
                    if (!prettyEvents) {
                        synchronized (eventStream) {
                            try {
                                if (value == null) {
                                    eventStream.write((time + ":" + evt.name() + "\n").getBytes());
                                } else {
                                    eventStream.write((time + ":" + evt.name() + ":" + value + "\n").getBytes());
                                }
                            } catch (IOException ex) {
                                Logger.getLogger(CommandLineRequestExecutor.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    } else {
                        Date date = new Date(time);
                        ANSICode color = ANSICode.CYAN;
                        if (evt == Event.id || evt == Event.running) {
                            color = ANSICode.GREEN;
                        } else if (evt == Event.queued) {
                            color = ANSICode.YELLOW;
                        } else if (evt == Event.cancelled) {
                            color = ANSICode.YELLOW;
                        } else if (evt == Event.retcode) {
                            if (value.equals("0")) {
                                color = ANSICode.GREEN;
                            } else {
                                color = ANSICode.RED;
                            }
                        } else if (evt == Event.error) {
                            color = ANSICode.RED;
                            if (value != null) {
                                try {
                                    JsonNode node = JsonCodec.getInstance().parse(value);
                                    value = node.asString();
                                } catch (ParseException ex) {
                                    Logger.getLogger(CommandLineRequestExecutor.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                        } else if (evt == Event.shutdown) {
                            color = ANSICode.RED;
                        }
                        synchronized (eventStream) {
                            try {
                                eventStream.write((color.getCode() + "[wava] [" + Utils.DATE_FORMAT.format(date) + "] [" + evt + (value != null ? (":" + value) : "") + "]" + ANSICode.RESET.getCode() + "\n").getBytes());
                            } catch (IOException ex) {
                                Logger.getLogger(CommandLineRequestExecutor.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                }
            };
        }
        LineListener sterrListener = new LineListener() {
            @Override
            public void onNewLine(String line) {
                System.err.println(line);
            }
        };
        InputStream stdinStream;
        if (System.getenv(EnvEntry.STDIN_TTY.name()) != null) {
            stdinStream = null;
        } else {
            stdinStream = System.in;
        }
        return super.executeRequest(opName, input, stdinStream, System.out, sterrListener, evtListener);
    }
}
