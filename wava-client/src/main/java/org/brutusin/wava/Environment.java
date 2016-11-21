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
package org.brutusin.wava;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public final class Environment {

    private final static Logger LOGGER = Logger.getLogger(Environment.class.getName());

    public static final File ROOT;
    public static final File TEMP;
    public static final String WAVA_HOME = "WAVA_HOME";
    public static final String WAVA_JOB_ID = "WAVA_JOB_ID";

    static {
        String value = System.getenv(WAVA_HOME);
        if (value == null) {
            LOGGER.log(Level.SEVERE, "Enviroment variable not found: " + WAVA_HOME);
            throw new Error("Enviroment variable not found: " + WAVA_HOME);
        }
        ROOT = new File(value);
        TEMP = new File("/dev/shm/wava");
    }

    private Environment() {
    }
}
