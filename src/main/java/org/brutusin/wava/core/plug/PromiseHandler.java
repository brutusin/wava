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
package org.brutusin.wava.core.plug;

import java.io.IOException;
import org.brutusin.wava.core.Scheduler;
import org.brutusin.wava.core.cfg.Config;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public abstract class PromiseHandler {

    private static final PromiseHandler INSTANCE;

    static {
        try {
            INSTANCE = (PromiseHandler) Class.forName(Config.getInstance().getSchedulerCfg().getPromiseHandlerClassName()).newInstance();
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    public static PromiseHandler getInstance() {
        return INSTANCE;
    }

    /**
     *
     * @param availableMemory
     * @param pi
     * @param treeRSS
     * @return true if the process is allowed to continue executing, false
     * otherwise
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract boolean promiseFailed(long availableMemory, Scheduler.ProcessInfo pi, long treeRSS) throws IOException, InterruptedException;
}
