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
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;

/**
 * 
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 * @param <T> 
 */
public class PeerChannel<T> {

    private final String user;
    private final T request;

    private final FileOutputStream lifeCycleOs;
    private final FileOutputStream stdoutOs;
    private final FileOutputStream stderrOs;

    public PeerChannel(String user, T input, File namedPipesRoot) throws IOException, InterruptedException {
        this.user = user;
        this.request = input;
        File lifeCycleNamedPipe = new File(namedPipesRoot, "lifecycle");
        File stdoutNamedPipe = new File(namedPipesRoot, "stdout");
        File stderrNamedPipe = new File(namedPipesRoot, "stderr");
        this.lifeCycleOs = new FileOutputStream(lifeCycleNamedPipe);
        this.stdoutOs = new FileOutputStream(stdoutNamedPipe);
        this.stderrOs = new FileOutputStream(stderrNamedPipe);
        Miscellaneous.deleteDirectory(namedPipesRoot);
    }
    
    private static boolean writeSilently(OutputStream os, String message) {
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

    public OutputStream getLifeCycleOs() {
        return lifeCycleOs;
    }

    public OutputStream getStdoutOs() {
        return stdoutOs;
    }

    public OutputStream getStderrOs() {
        return stderrOs;
    }

    public boolean sendLogToPeer(Event event, String value) {
        if (value == null) {
            return writeSilently(lifeCycleOs, event + ":" + System.currentTimeMillis());
        } else {
            return writeSilently(lifeCycleOs, event + ":" + System.currentTimeMillis() + ":" + JsonCodec.getInstance().transform(value));
        }
    }

    public T getRequest() {
        return request;
    }

    public void close() throws IOException {
        this.lifeCycleOs.close();
        this.stdoutOs.close();
        this.stderrOs.close();
    }
}
