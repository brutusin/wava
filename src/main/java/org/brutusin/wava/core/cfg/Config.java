package org.brutusin.wava.core.cfg;

import org.brutusin.wava.core.cfg.impl.ConfigImpl;
import java.io.File;
import java.io.FileInputStream;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.core.Environment;

public class Config {

    private static final Config INSTANCE = new Config();
    private final ConfigImpl impl;

    private Config() {
        try {
            File cfgFile = new File(Environment.ROOT, "cfg/wava.json");
            if (!cfgFile.exists()) {
                throw new Error("Config file not found " + cfgFile.getAbsolutePath());
            }
            String str = Miscellaneous.toString(new FileInputStream(cfgFile), "UTF-8");
            this.impl = JsonCodec.getInstance().parse(replaceEnvEntries(str), ConfigImpl.class);
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    private static String replaceEnvEntries(String str) {
        StringBuilder sb = new StringBuilder(str);
        for (String envEntry : System.getenv().keySet()) {
            String entryName = "$" + envEntry;
            int i = sb.indexOf(entryName);
            if (i >= 0) {
                sb.replace(i, i + entryName.length(), System.getenv().get(envEntry));
            }
        }
        return sb.toString();
    }

    public static Config getInstance() {
        return INSTANCE;
    }

    public SchedulerCfg getSchedulerCfg() {
        return impl.getSchedulerCfg();
    }

    public ProcessCfg getProcessCfg() {
        return impl.getProcessCfg();
    }

    public GroupCfg getGroupCfg() {
        return impl.getGroupCfg();
    }

    public UICfg getuICfg() {
        return impl.getuICfg();
    }
}
