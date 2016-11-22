package org.brutusin.wava;

import org.brutusin.wava.env.WavaTemp;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.brutusin.commons.utils.Miscellaneous;

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
/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class Utils {

    public final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static List<String> parseEventLine(String line) {
        List<String> ret = new ArrayList<>();
        int start = 0;
        boolean inString = false;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '"' && (i == 0 || line.charAt(i - 1) != '\\')) {
                inString = !inString;
            }
            if (!inString && line.charAt(i) == ':') {
                ret.add(line.substring(start, i));
                start = i + 1;
            }
        }
        if (start < line.length()) {
            ret.add(line.substring(start));
        }
        return ret;
    }

    public static FileLock tryLock(File f) throws IOException {
        if (!f.exists()) {
            Miscellaneous.createFile(f.getAbsolutePath());
        }
        RandomAccessFile raf = new RandomAccessFile(f, "rws");
        return raf.getChannel().tryLock();
    }

    public static boolean isCoreRunning() throws IOException {
        return tryLock(new File(WavaTemp.getInstance().getTemp(), ".lock")) == null;
    }
}
