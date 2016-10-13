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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.wava.utils.ANSIColor;
import org.brutusin.wava.utils.Utils;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 * @param <T>
 */
public class PeerChannel<T> {

    public enum NamedPipe {

        events,
        stdout,
        stderr
    }
    private final String user;
    private final T request;
    private boolean closed = false;

    private final FileOutputStream eventsOs;
    private final FileOutputStream stdoutOs;
    private final FileOutputStream stderrOs;

    public PeerChannel(String user, T input, File namedPipesRoot) throws IOException, InterruptedException {
        this.user = user;
        this.request = input;
        File eventsNamedPipe = new File(namedPipesRoot, NamedPipe.events.name());
        File stdoutNamedPipe = new File(namedPipesRoot, NamedPipe.stdout.name());
        File stderrNamedPipe = new File(namedPipesRoot, NamedPipe.stderr.name());
        this.eventsOs = new FileOutputStream(eventsNamedPipe);
        this.stdoutOs = new FileOutputStream(stdoutNamedPipe);
        this.stderrOs = new FileOutputStream(stderrNamedPipe);
        Miscellaneous.deleteDirectory(namedPipesRoot);
    }

    public static boolean println(OutputStream os, String message) {
        try {
            os.write(message.getBytes());
            os.write("\n".getBytes());
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

    public synchronized boolean log(ANSIColor color, String message) {
        if (closed) {
            return false;
        }
        return println(stderrOs, color + "[wava] [" + Utils.DATE_FORMAT.format(new Date()) + "] " + message + ANSIColor.RESET);
    }

    public boolean ping() {
        return sendEvent(Event.ping, null);
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

    public T getRequest() {
        return request;
    }

    public synchronized void close() throws IOException {
        this.closed = true;
        this.eventsOs.close();
        this.stdoutOs.close();
        this.stderrOs.close();
    }
}
