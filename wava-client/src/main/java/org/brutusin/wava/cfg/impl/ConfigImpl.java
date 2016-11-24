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

import java.io.File;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class ConfigImpl {

    private String tempFolder;
    private UICfgImpl uICfg = new UICfgImpl();
    private SchedulerCfgImpl schedulerCfg = new SchedulerCfgImpl();
    private ProcessCfgImpl processCfg = new ProcessCfgImpl();
    private GroupCfgImpl groupCfg = new GroupCfgImpl();

    public SchedulerCfgImpl getSchedulerCfg() {
        return schedulerCfg;
    }

    public void setSchedulerCfg(SchedulerCfgImpl schedulerCfg) {
        this.schedulerCfg = schedulerCfg;
    }

    public ProcessCfgImpl getProcessCfg() {
        return processCfg;
    }

    public void setProcessCfg(ProcessCfgImpl processCfg) {
        this.processCfg = processCfg;
    }

    public GroupCfgImpl getGroupCfg() {
        return groupCfg;
    }

    public void setGroupCfg(GroupCfgImpl groupCfg) {
        this.groupCfg = groupCfg;
    }

    public UICfgImpl getuICfg() {
        return uICfg;
    }

    public void setuICfg(UICfgImpl uICfg) {
        this.uICfg = uICfg;
    }

    public String getTempFolder() {
        return tempFolder;
    }

    public void setTempFolder(String tempFolder) {
        this.tempFolder = tempFolder;
    }
}
