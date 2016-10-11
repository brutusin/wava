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
import org.brutusin.wava.data.Stats;
import org.brutusin.wava.data.ANSIColor;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class StrictPromiseHandler extends PromiseHandler {

    @Override
    public void promiseFailed(long availableMemory, Scheduler.ProcessInfo pi, Stats processStats) throws IOException, InterruptedException {
        pi.getChannel().log(ANSIColor.YELLOW, "memory promise excedeed");
        LinuxCommands.getInstance().killTree(pi.getPid());
    }
}