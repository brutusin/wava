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
package org.brutusin.scheduler.core.plug.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import org.brutusin.scheduler.core.plug.LinuxCommands;
import org.brutusin.scheduler.data.Stats;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class CachingLinuxCommands extends LinuxCommands {
    
    private final LinuxCommands commands;
    private final Cache cache;
    
    public CachingLinuxCommands(LinuxCommands commands, int timeToLiveSeconds) {
        this.commands = commands;
        CacheManager cm = CacheManager.create();
        this.cache = new Cache(
                new CacheConfiguration(CachingLinuxCommands.class.getName() + "-cache", 0)
                .timeToLiveSeconds(timeToLiveSeconds)
                .persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));
        cm.addCache(cache);
    }
    
    @Override
    public void killTree(int pid) throws IOException, InterruptedException {
        this.commands.killTree(pid);
    }
    
    @Override
    public Map<Integer, Stats> getStats(int[] pIds) throws IOException, InterruptedException {
        String key = Arrays.toString(pIds);
        Element element = cache.get(key);
        if (element == null) {
            synchronized (cache) {
                element = cache.get(key);
                if (element == null) {
                    Map<Integer, Stats> stats = commands.getStats(pIds);
                    element = new Element(key, stats);
                    cache.put(element);
                }
            }
        }
        return (Map<Integer, Stats>) element.getObjectValue();
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
    
    @Override
    public void createNamedPipes(File... files) throws IOException, InterruptedException {
        commands.createNamedPipes(files);
    }
}
