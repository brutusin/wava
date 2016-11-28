c/*
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

import org.brutusin.wava.cfg.Config;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public abstract class NicenessHandler {

    private volatile static NicenessHandler instance;

    public static NicenessHandler getInstance() {
        if (instance == null) {
            synchronized (NicenessHandler.class) {
                if (instance == null) {
                    try {
                        instance = (NicenessHandler) Class.forName(Config.getInstance().getSchedulerCfg().getNicenessHandlerClassName()).newInstance();
                    } catch (Exception ex) {
                        throw new Error(ex);
                    }
                }
            }
        }
        return instance;
    }

    /**
     * Returns the niceness of the i-th process out of a total of {@code total}
     *
     * @param i from {@code 0} to {@code total -1}, in decreasing order of
     * priority
     * @param total total number of running processes
     * @param minNiceness
     * @param maxNiceness
     * @return
     */
    public abstract int getNiceness(int i, int total, int minNiceness, int maxNiceness);
}
