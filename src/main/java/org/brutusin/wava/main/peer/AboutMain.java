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
package org.brutusin.wava.main.peer;

import java.io.File;
import org.brutusin.wava.core.Environment;
import org.brutusin.wava.utils.Utils;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class AboutMain {

    public static void main(String[] args) throws Exception {
        System.err.println(Utils.getLogo());
        System.err.println("Project site:\n\t https://github.com/brutusin/wava");
        System.err.println("Created by:\n\t Ignacio del Valle Alles");
        System.err.println("Offline help:\n\t more " + new File(Environment.ROOT, "README").getAbsolutePath());
    }
}
