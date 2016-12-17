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
package org.brutusin.wava.core.plug.impl.promise;

import java.io.IOException;
import org.brutusin.wava.core.plug.PromiseHandler;
import org.brutusin.wava.core.Scheduler;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.io.Event;

/**
 * Allows the execution of promise-failing jobs as long as the total-max-RSS and
 * max-RSS-per-job thresholds are not overpassed.
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class LaxPromiseHandler extends PromiseHandler {

    @Override
    public FailingAction promiseFailed(long availableMemory, Scheduler.ProcessInfo pi, long currentTotalUsedRss, long schedulerManagedRss) throws IOException, InterruptedException {
        if (currentTotalUsedRss < schedulerManagedRss) {
            return FailingAction.tolerate;
        } else if (Config.getInstance().getSchedulerCfg().getMaxJobRSSBytes() > 0 && pi.getMaxSeenRSS() > Config.getInstance().getSchedulerCfg().getMaxJobRSSBytes()) {
            pi.getJobInfo().getSubmitChannel().sendEvent(Event.exceed_global, Config.getInstance().getSchedulerCfg().getMaxJobRSSBytes());
            return FailingAction.kill;
        } else {
            return FailingAction.reenqueue;
        }
    }
}
