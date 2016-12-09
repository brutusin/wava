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
package org.brutusin.wava.core.plug.impl.niceness;

import org.brutusin.wava.core.plug.NicenessHandler;

public final class GroupHomogeneusSpreadNicenessHandler extends NicenessHandler {

    @Override
    public int getNiceness(int processPosition, int processCount, int groupPosition, int groupCount, int minNiceness, int maxNiceness) {
        return HomogeneusSpreadNicenessHandler.distribute(groupPosition, groupCount, minNiceness, maxNiceness);
    }
}
