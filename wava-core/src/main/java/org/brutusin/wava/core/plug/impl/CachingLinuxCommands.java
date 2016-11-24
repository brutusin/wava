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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import org.brutusin.wava.cfg.Config;
import org.brutusin.wava.core.plug.LinuxCommands;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class CachingLinuxCommands extends LinuxCommands {

    private final LinuxCommands commands;
    private final Cache cache;

    public CachingLinuxCommands(LinuxCommands commands) {
        this.commands = commands;
        CacheManager cm = CacheManager.create();
        this.cache = new Cache(
                new CacheConfiguration(CachingLinuxCommands.class.getName() + "-cache", 0)
                .timeToLiveSeconds(Config.getInstance().getSchedulerCfg().getCommandTTLCacheSecs())
                .persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));
        cm.addCache(cache);
    }

    @Override
    public void setNiceness(int pId, int niceness) throws IOException, InterruptedException {
        this.commands.setNiceness(pId, niceness);
    }

    @Override
    public void setImmutable(File f, boolean immutable) throws IOException, InterruptedException {
        this.commands.setImmutable(f, immutable);
    }

    @Override
    public void killTree(int pid) throws IOException, InterruptedException {
        this.commands.killTree(pid);
    }

    @Override
    public String[] decorateWithCPUAffinity(String[] cmd, String affinity) {
        return this.commands.decorateWithCPUAffinity(cmd, affinity);
    }

    @Override
    public long[] getTreeRSS(int[] pIds) throws IOException, InterruptedException {
        String key = Arrays.toString(pIds);
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    long[] ret = commands.getTreeRSS(pIds);
                    element = new Element(key, ret);
                    cache.put(element);
                }
            }
        }
        return (long[]) element.getObjectValue();
    }

    @Override
    public long getSystemRSSUsedMemory() throws IOException, InterruptedException {
        String key = "getSystemRSSUsedMemory";
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    long value = commands.getSystemRSSUsedMemory();
                    element = new Element(key, value);
                    cache.put(element);
                }
            }
        }
        return (Long) element.getObjectValue();
    }

    @Override
    public long getSystemRSSFreeMemory() throws IOException, InterruptedException {
        String key = "getSystemRSSFreeMemory";
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    long value = commands.getSystemRSSFreeMemory();
                    element = new Element(key, value);
                    cache.put(element);
                }
            }
        }
        return (Long) element.getObjectValue();
    }

    @Override
    public long getSystemRSSMemory() throws IOException, InterruptedException {
        String key = "getSystemRSSMemory";
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    long value = commands.getSystemRSSMemory();
                    element = new Element(key, value);
                    cache.put(element);
                }
            }
        }
        return (Long) element.getObjectValue();
    }

    @Override
    public String getFileOwner(File f) throws IOException, InterruptedException {
        String key = f.getAbsolutePath();
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    String value = commands.getFileOwner(f);
                    element = new Element(key, value);
                    cache.put(element);
                }
            }
        }
        return (String) element.getObjectValue();
    }

    @Override
    public String getRunningUser() throws IOException, InterruptedException {
        String key = "getRunningUser";
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    String value = commands.getRunningUser();
                    element = new Element(key, value);
                    element.setEternal(true);
                    cache.put(element);
                }
            }
        }
        return (String) element.getObjectValue();
    }

    @Override
    public String[] getRunAsCommand(String user, String[] cmd) {
        return commands.getRunAsCommand(user, cmd);
    }
}
