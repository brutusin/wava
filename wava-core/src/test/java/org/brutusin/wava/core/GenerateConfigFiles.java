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
package org.brutusin.wava.core;

import java.io.File;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.cfg.impl.ConfigImpl;
import org.brutusin.wava.core.plug.impl.HomogeneusSpreadNicenessHandler;
import org.brutusin.wava.core.plug.impl.LaxPromiseHandler;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class GenerateConfigFiles {

    public static void main(String[] args) throws Exception {
        File cfgFile = new File("cfg.json");
        ConfigImpl defaultImpl = new ConfigImpl();
        defaultImpl.setTempFolder("/dev/shm/wava/$WAVA_HOME");
        defaultImpl.getSchedulerCfg().setPromiseHandlerClassName(LaxPromiseHandler.class.getName());
        defaultImpl.getSchedulerCfg().setNicenessHandlerClassName(HomogeneusSpreadNicenessHandler.class.getName());
        Miscellaneous.writeStringToFile(cfgFile, JsonCodec.getInstance().prettyPrint(JsonCodec.getInstance().transform(defaultImpl)), "UTF-8");
    }
}
