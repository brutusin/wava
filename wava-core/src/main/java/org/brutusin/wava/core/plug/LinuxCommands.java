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

import java.io.File;
import java.io.IOException;
import org.brutusin.wava.core.plug.impl.CachingLinuxCommands;
import org.brutusin.wava.core.plug.impl.POSIXCommandsImpl;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public abstract class LinuxCommands {

    private static final LinuxCommands INSTANCE = new CachingLinuxCommands(new POSIXCommandsImpl());

    public static LinuxCommands getInstance() {
        return INSTANCE;
    }

    public abstract void setImmutable(File f, boolean immutable) throws IOException, InterruptedException;
    
    public abstract void setNiceness(int pId, int niceness) throws IOException, InterruptedException;
    
    public abstract String[] decorateWithCPUAffinity(String[] cmd, String affinity);
    
    public abstract long[] getTreeRSS(int[] pids) throws IOException, InterruptedException;
    
    public abstract void killTree(int pid) throws IOException, InterruptedException;

    public abstract long getSystemRSSUsedMemory() throws IOException, InterruptedException;

    public abstract long getSystemRSSFreeMemory() throws IOException, InterruptedException;

    public abstract long getSystemRSSMemory() throws IOException, InterruptedException;

    public abstract String[] getRunAsCommand(String user, String[] cmd);
    
    public abstract String getFileOwner(File f) throws IOException, InterruptedException;

    public abstract String getRunningUser() throws IOException, InterruptedException;

}
