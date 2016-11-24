package org.brutusin.wava.cfg;

import org.brutusin.wava.cfg.impl.ConfigImpl;
import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.env.WavaHome;

public class Config {

    private final File tempFolder;
    private final static Logger LOGGER = Logger.getLogger(Config.class.getName());
    private static volatile Config instance;
    private final ConfigImpl impl;

    private Config() {
        try {
            File cfgFile = new File(WavaHome.getInstance().getFile(), "cfg/wava.json");
            if (!cfgFile.exists()) {
                throw new RuntimeException("Config file not found " + cfgFile.getAbsolutePath());
            }
            String str = Miscellaneous.toString(new FileInputStream(cfgFile), "UTF-8");
            this.impl = JsonCodec.getInstance().parse(replaceEnvEntries(str), ConfigImpl.class);
            if (impl.getTempFolder() != null) {
                this.tempFolder = new File(impl.getTempFolder());
            } else {
                this.tempFolder = null;
            }
        } catch (Throwable th) {
            LOGGER.log(Level.SEVERE, th.getMessage(), th);
            throw new Error(th);
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
        if (instance == null) {
            synchronized (Config.class) {
                if (instance == null) {
                    instance = new Config();
                }
            }
        }
        return instance;
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

    public File getTempFolder() {
        return this.tempFolder;
    }
}
