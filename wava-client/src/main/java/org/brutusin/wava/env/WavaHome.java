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
package org.brutusin.wava.env;

import java.io.File;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class WavaHome {

    private static volatile WavaHome instance;

    private final File file;

    public static WavaHome getInstance() {
        if (instance == null) {
            synchronized (WavaHome.class) {
                if (instance == null) {
                    instance = new WavaHome();
                }
            }
        }
        return instance;
    }

    private WavaHome() {
        String wavaHome = System.getenv(EnvEntry.WAVA_HOME.name());
        if (wavaHome == null) {
            throw new WavaHomeNotSetError();
        }
        this.file = new File(wavaHome);
    }

    public File getFile() {
        return file;
    }

    private static final String removeLeadingAndTrailingSlashes(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    public String getId() {
        return "wava[" + removeLeadingAndTrailingSlashes(file.getAbsolutePath()).replace("/", ".")+"]";
    }
}
