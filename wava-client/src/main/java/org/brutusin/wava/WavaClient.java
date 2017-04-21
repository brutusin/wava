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
package org.brutusin.wava;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.brutusin.wava.env.EnvEntry;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.ExtendedSubmitInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.SubmitInput;
import org.brutusin.wava.io.EventListener;
import org.brutusin.wava.io.LineListener;
import org.brutusin.wava.io.OpName;
import org.brutusin.wava.io.RequestExecutor;
import org.brutusin.wava.io.RetCode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class WavaClient {

    private final RequestExecutor executor = new RequestExecutor();

    public void submit(SubmitInput input, final InputStream stdinStream, final OutputStream stdoutStream, final LineListener stderrListener, final EventListener eventListener) throws WavaNotRunningException {
        try {
            ExtendedSubmitInput esi = new ExtendedSubmitInput(input);
            String parentId = System.getenv(EnvEntry.WAVA_JOB_ID.name());
            if (parentId != null) {
                esi.setParentId(Integer.valueOf(parentId));
            }
            Integer retCode = executor.executeRequest(OpName.submit, esi, stdinStream, stdoutStream, stderrListener, eventListener);
            if (retCode == RetCode.CORE_NOT_RUNNING.getCode()) {
                throw new WavaNotRunningException();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static String executeCommand(RequestExecutor executor, OpName opName, Object input) throws WavaNotRunningException {
        ByteArrayOutputStream stdoutOs = new ByteArrayOutputStream();
        final StringBuilder sb = new StringBuilder();
        LineListener stderrListener = new LineListener() {
            @Override
            public void onNewLine(String line) {
                if (sb.length() > 0) {
                    sb.append("\n");
                }
                sb.append(line);
            }
        };
        try {
            Integer retCode = executor.executeRequest(opName, input, null, stdoutOs, stderrListener, null);
            if (retCode == RetCode.CORE_NOT_RUNNING.getCode()) {
                throw new WavaNotRunningException();
            } else if (retCode != 0) {
                throw new RuntimeException(sb.toString());
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return stdoutOs.toString();
    }

    public String executeGroupCommand(GroupInput input) throws WavaNotRunningException {
        return executeCommand(executor, OpName.group, input);
    }

    public String cancelJobCommand(CancelInput input) throws WavaNotRunningException {
        return executeCommand(executor, OpName.cancel, input);
    }

    public static boolean isSchedulerRunning() {
        try {
            return Utils.isCoreRunning();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
