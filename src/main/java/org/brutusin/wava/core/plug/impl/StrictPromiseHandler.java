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
package org.brutusin.wava.core.plug.impl;

import java.io.IOException;
import org.brutusin.wava.core.Event;
import org.brutusin.wava.core.plug.PromiseHandler;
import org.brutusin.wava.core.Scheduler;
import org.brutusin.wava.core.plug.LinuxCommands;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class StrictPromiseHandler extends PromiseHandler {

    @Override
    public void promiseFailed(long availableMemory, Scheduler.ProcessInfo pi, long treeRSS) throws IOException, InterruptedException {
        pi.getJobInfo().getSubmitChannel().sendEvent(Event.exceed, pi.getJobInfo().getSubmitChannel().getRequest().getMaxRSS());
        LinuxCommands.getInstance().killTree(pi.getPid());
    }
}
