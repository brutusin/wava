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
package org.brutusin.wava.utils;

import org.brutusin.wava.core.cfg.Config;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public enum ANSICode {

    BLACK("\u001B[30m"),
    RED("\u001B[31m"),
    GREEN("\u001B[32m"),
    YELLOW("\u001B[33m"),
    BLUE("\u001B[34m"),
    PURPLE("\u001B[35m"),
    CYAN("\u001B[36m"),
    WHITE("\u001B[37m"),
    BG_GREEN("\u001b[42m"),
    BG_BLACK("\u001b[40m"),
    BG_RED("\u001b[41m"),
    BG_YELLOW("\u001b[43m"),
    BG_BLUE("\u001b[44m"),
    BG_MAGENTA("\u001b[45m"),
    BG_CYAN("\u001b[46m"),
    BG_WHITE("\u001b[47m"),
    RESET("\u001B[0m"),
    BOLD("\u001b[1m"),
    UNDERLINED("\u001b[4m"),
    BLINK("\u001b[5m"),
    REVERSED("\u001b[7m"),
    INVISIBLE("\u001b[8m"),
    END_OF_LINE("\u001b[K"),
    MOVE_TO_TOP("\u001b[0;0f"),
    CLEAR("\u001b[2J");

    private final String code;

    ANSICode(String code) {
        this.code = code;
    }

    public String getCode() {
        if (Config.getInstance().isAnsiColors()) {
            return code;
        } else {
            return "";
        }
    }
}
