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

import org.brutusin.wava.core.plug.NicenessHandler;

/**
 * Chooses the most favorable niceness value from those values with the minimum
 * multiplicity (collisions due to the pigeonhole principle)
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class HomogeneusSpreadNicenessHandler extends NicenessHandler {

    @Override
    public int getNiceness(int i, int total, int minNiceness, int maxNiceness) {
        int s = maxNiceness - minNiceness + 1;
        int r = (total - 1) / s + 1;
        int c = total - (s * (r - 1));
        int l = c * r;
        if (i + 1 <= l) {
            return minNiceness + i / r;
        } else {
            return minNiceness + c + (i - l) / (r - 1);
        }
    }
}
