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
package org.brutusin.scheduler.core.plug.impl;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.scheduler.core.plug.PromiseHandler;
import org.brutusin.scheduler.core.Scheduler;
import org.brutusin.scheduler.data.Stats;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class StrictPromiseHandler extends PromiseHandler{

    public void promiseFailed(long availableMemory, Scheduler.ProcessInfo pi, Stats processStats) {
        try {
            pi.getJobInfo().getLifeCycleOs().write("memory promise excedeed!".getBytes());
            pi.getProcess().destroy();
        } catch (IOException ex) {
            Logger.getLogger(StrictPromiseHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
