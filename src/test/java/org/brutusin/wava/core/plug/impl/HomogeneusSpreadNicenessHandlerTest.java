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

import org.junit.Test;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class HomogeneusSpreadNicenessHandlerTest {

    public HomogeneusSpreadNicenessHandlerTest() {
    }

    @Test
    public void testGetNiceness() {
        HomogeneusSpreadNicenessHandler handler = new HomogeneusSpreadNicenessHandler();
        int minNiceness = -20;
        int maxNiceness = 19;
        for (int total = 0; total < 100; total++) {
            System.out.print(total + ":");
            for (int i = 0; i < total; i++) {
                System.out.print(" " + handler.getNiceness(i, total, minNiceness, maxNiceness));
            }
            System.out.println();
        }
    }
}
