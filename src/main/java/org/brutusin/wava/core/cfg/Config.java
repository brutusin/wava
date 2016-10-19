package org.brutusin.wava.core.cfg;

import java.io.File;
import java.io.FileInputStream;
import org.brutusin.commons.utils.Miscellaneous;
import org.brutusin.json.spi.JsonCodec;
import org.brutusin.wava.core.Environment;

public final class Config {

    private static final Config INSTANCE = new Config();
    private final ConfigImpl impl;

    private Config() {
        try {
            File cfgFile = new File(Environment.ROOT, "cfg/wava.json");
            if (!cfgFile.exists()) {
                this.impl = createDefaultCfg();
                Miscellaneous.writeStringToFile(cfgFile, JsonCodec.getInstance().prettyPrint(JsonCodec.getInstance().transform(this.impl)), "UTF-8");
            } else {
                String str = Miscellaneous.toString(new FileInputStream(cfgFile), "UTF-8");
                this.impl = JsonCodec.getInstance().parse(str, ConfigImpl.class);
            }
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    public boolean isAnsiColors() {
        return impl.isAnsiColors();
    }

    private ConfigImpl createDefaultCfg() {
        ConfigImpl ret = new ConfigImpl();
        return ret;
    }

    public static Config getInstance() {
        return INSTANCE;
    }

    public int getPollingSecs() {
        return impl.getPollingSecs();
    }

    public int getMaxTotalRSSBytes() {
        return impl.getMaxTotalRSSBytes();
    }
    
    public int getCommandTTLCacheSecs() {
        return impl.getCommandTTLCacheSecs();
    }

    public int getSigKillDelaySecs() {
        return impl.getSigKillDelaySecs();
    }

    public String getCpuAfinity() {
        return impl.getCpuAfinity();
    }

    public int getDynamicGroupIdleSeconds() {
        return impl.getDynamicGroupIdleSeconds();
    }

    public int[] getNicenessRange() {
        return impl.getNicenessRange();
    }
}
