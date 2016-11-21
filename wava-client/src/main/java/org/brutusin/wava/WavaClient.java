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
import java.io.OutputStream;
import org.brutusin.wava.input.CancelInput;
import org.brutusin.wava.input.GroupInput;
import org.brutusin.wava.input.SubmitInput;
import org.brutusin.wava.io.EventListener;
import org.brutusin.wava.io.OpName;
import org.brutusin.wava.io.RequestExecutor;
import org.brutusin.wava.io.RetCode;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class WavaClient {

    private final RequestExecutor executor = new RequestExecutor();

    public void submit(SubmitInput input, final OutputStream stdoutStream, final OutputStream stderrStream, final EventListener eventListener) throws WavaNotRunningException {
        int retCode;
        try {
            retCode = executor.executeRequest(OpName.submit, input, stdoutStream, stderrStream, eventListener);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        if (retCode == RetCode.CORE_NOT_RUNNING.getCode()) {
            throw new WavaNotRunningException();
        }
    }

    private static String executeCommand(RequestExecutor executor, OpName opName, Object input) throws WavaNotRunningException {
        ByteArrayOutputStream stdoutOs = new ByteArrayOutputStream();
        ByteArrayOutputStream stderrOs = new ByteArrayOutputStream();
        int retCode;
        try {
            retCode = executor.executeRequest(opName, input, stdoutOs, stderrOs, null);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        if (retCode == RetCode.CORE_NOT_RUNNING.getCode()) {
            throw new WavaNotRunningException();
        } else if (retCode != 0) {
            throw new RuntimeException(stderrOs.toString());
        }
        return stderrOs.toString();
    }

    public String executeGroupCommand(GroupInput input) throws WavaNotRunningException {
        return executeCommand(executor, OpName.group, input);
    }

    public String cancelJobCommand(CancelInput input) throws WavaNotRunningException {
        return executeCommand(executor, OpName.cancel, input);
    }

    public boolean isSchedulerRunning() {
        try {
            return Utils.isCoreRunning();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
