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
package org.brutusin.wava.cfg.impl;

import org.brutusin.wava.cfg.ProcessCfg;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class ProcessCfgImpl implements ProcessCfg {

    private int[] nicenessRange = {1, 19};
    private String cpuAfinity = "$DEFAULT_CPU_AFINITY";

    @Override
    public int[] getNicenessRange() {
        return nicenessRange;
    }

    public void setNicenessRange(int[] nicenessRange) {
        this.nicenessRange = nicenessRange;
    }

    @Override
    public String getCpuAfinity() {
        return cpuAfinity;
    }

    public void setCpuAfinity(String cpuAfinity) {
        this.cpuAfinity = cpuAfinity;
    }
}
