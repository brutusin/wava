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
package org.brutusin.wava.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import org.brutusin.commons.Bean;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.commons.utils.ProcessUtils;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.Environment;
import org.brutusin.wava.Utils;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class RequestExecutor {

    public Integer executeRequest(OpName opName, Object input, final OutputStream stdoutStream, final OutputStream stderrStream, final EventListener eventListener) throws IOException, InterruptedException {
        File counterFile = new File(Environment.TEMP, "state/.seq");
        long id = Miscellaneous.getGlobalAutoIncremental(counterFile);
        String json = JsonCodec.getInstance().transform(input);
        File streamRoot = new File(Environment.TEMP, "streams/" + id);
        Miscellaneous.createDirectory(streamRoot);
        File eventsNamedPipe = new File(streamRoot, NamedPipe.events.name());
        File stdoutNamedPipe = new File(streamRoot, NamedPipe.stdout.name());
        File stderrNamedPipe = new File(streamRoot, NamedPipe.stderr.name());
        ProcessUtils.createPOSIXNamedPipes(eventsNamedPipe, stderrNamedPipe, stdoutNamedPipe);
        final Bean<Integer> retCode = new Bean<>();
        Thread eventsThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream eventsIs = new FileInputStream(eventsNamedPipe);
                    BufferedReader br = new BufferedReader(new InputStreamReader(eventsIs));
                    String line;
                    while ((line = br.readLine()) != null) {
                        List<String> tokens = Utils.parseEventLine(line);
                        Event evt = Event.valueOf(tokens.get(1));
                        String value;
                        if (tokens.size() > 2) {
                            value = tokens.get(2);
                            if (evt == Event.retcode) {
                                retCode.setValue(Integer.valueOf(value));
                            }
                        } else {
                            value = null;
                        }
                        if (eventListener != null) {
                            eventListener.onEvent(evt, value, Long.valueOf(tokens.get(0)));
                        }
                    }
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                }
            }
        };
        eventsThread.start();
        Thread outThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream is = new FileInputStream(stdoutNamedPipe);
                    Miscellaneous.pipeAsynchronously(is, false, stdoutStream);
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                }
            }
        };
        outThread.start();
        Thread errThread = new Thread() {
            @Override
            public void run() {
                try {
                    InputStream is = new FileInputStream(stderrNamedPipe);
                    Miscellaneous.pipeAsynchronously(is, false, stderrStream);
                } catch (Throwable th) {
                    th.printStackTrace(System.err);
                }
            }
        };
        errThread.start();

        File tempFile = new File(Environment.TEMP, "temp/" + id + "-" + opName);
        File requestFile = new File(Environment.TEMP, "request/" + id + "-" + opName);

        Files.write(tempFile.toPath(), json.getBytes());
        Files.move(tempFile.toPath(), requestFile.toPath());
        if (eventsThread
                != null) {

        }
        errThread.join();
        eventsThread.join();
        outThread.join();

        return retCode.getValue();
    }

}
