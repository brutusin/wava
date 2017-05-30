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

import org.brutusin.wava.io.Event;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.commons.Bean;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.input.Input;
import org.brutusin.wava.io.NamedPipe;
import org.brutusin.wava.utils.ANSICode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 * @param <I extends Input>
 */
public class PeerChannel<I extends Input> {

    private static final Logger LOGGER = Logger.getLogger(PeerChannel.class.getName());

    private final String user;
    private final I input;
    private final File procFile;
    private boolean closed = false;

    private final FileInputStream stdinIs;

    private final FileOutputStream eventsOs;
    private final FileOutputStream stdoutOs;
    private final FileOutputStream stderrOs;

    public PeerChannel(String user, I input, File namedPipesRoot) throws OrphanChannelException, IOException {
        this.user = user;
        this.input = input;
        this.procFile = new File("/proc/" + input.getClientPid());

        File stdinNamedPipe = new File(namedPipesRoot, NamedPipe.stdin.name());
        File eventsNamedPipe = new File(namedPipesRoot, NamedPipe.events.name());
        File stdoutNamedPipe = new File(namedPipesRoot, NamedPipe.stdout.name());
        File stderrNamedPipe = new File(namedPipesRoot, NamedPipe.stderr.name());

        final Bean<Boolean> initializedBean = new Bean<>();
        Thread timeoutThread = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(10 * 1000);
                        synchronized (initializedBean) {
                            if (initializedBean.getValue() == null) { // peer not read all pipes already
                                if (isPeerAlive()) { // peer still running
                                    continue;
                                } else { // peer not running -> no process reading pipe
                                    try {
                                        new FileInputStream(eventsNamedPipe).close();
                                    } catch (Exception ex) {
                                    }
                                    try {
                                        new FileInputStream(stdoutNamedPipe).close();
                                    } catch (Exception ex) {
                                    }
                                    try {
                                        new FileInputStream(stderrNamedPipe).close();
                                    } catch (Exception ex) {
                                    }
                                    try {
                                        new FileOutputStream(stdinNamedPipe).close();
                                    } catch (Exception ex) {
                                    }
                                }
                            } else {
                                return;
                            }
                        }
                    }
                } catch (InterruptedException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }
            }
        };
        timeoutThread.setDaemon(true);
        timeoutThread.start();
        this.eventsOs = new FileOutputStream(eventsNamedPipe);
        this.stdoutOs = new FileOutputStream(stdoutNamedPipe);
        this.stderrOs = new FileOutputStream(stderrNamedPipe);
        this.stdinIs = new FileInputStream(stdinNamedPipe);
        try {
            synchronized (initializedBean) {
                if (initializedBean.getValue() != null) {
                    throw new OrphanChannelException();
                }
                initializedBean.setValue(true);
            }
        } finally {
            Miscellaneous.deleteDirectory(namedPipesRoot);
        }
    }

    public static boolean println(OutputStream os, String message) {
        try {
            byte[] bytes = message.getBytes();
            for (int i = 0; i < bytes.length; i++) {
                os.write(bytes[i]);
                if (i > 0 && i % 1024 == 0) {
                    os.flush();
                }
            }
            os.write("\n".getBytes());
            os.flush();
            return true;
        } catch (IOException ex) {
            // Peer closed
            return false;
        }
    }

    public String getUser() {
        return user;
    }

    public OutputStream getEventsOs() {
        return eventsOs;
    }

    public OutputStream getStdoutOs() {
        return stdoutOs;
    }

    public OutputStream getStderrOs() {
        return stderrOs;
    }

    public InputStream getStdinIs() {
        return stdinIs;
    }

    public synchronized boolean sendMessage(ANSICode color, String message) {
        if (closed) {
            return false;
        }
        if (color == null) {
            color = ANSICode.RESET;
        }
        return println(stderrOs, color + message + ANSICode.RESET);
    }

    public boolean isPeerAlive() {
        return procFile.exists();
    }

    public synchronized boolean sendEvent(Event event, Object value) {
        if (closed) {
            return false;
        }
        if (value == null) {
            return println(eventsOs, System.currentTimeMillis() + ":" + event.name());
        } else {
            return println(eventsOs, System.currentTimeMillis() + ":" + event.name() + ":" + value);
        }
    }

    public I getInput() {
        return input;
    }

    public synchronized void close() throws IOException {
        this.closed = true;
        this.eventsOs.flush();
        this.stdoutOs.flush();
        this.stderrOs.flush();
        this.stdinIs.close();
        this.eventsOs.close();
        this.stdoutOs.close();
        this.stderrOs.close();
    }
}
