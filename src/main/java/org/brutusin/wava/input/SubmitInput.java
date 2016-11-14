package org.brutusin.wava.input;

import java.io.File;
import java.util.Map;

public class SubmitInput {

    private Integer parentId;
    private String groupName;
    private String[] command;
    private long maxRSS;    
    private Map<String,String> environment;    
    private File workingDirectory;

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public void setEnvironment(Map<String, String> environment) {
        this.environment = environment;
    }

    public File getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(File workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
    
    public String[] getCommand() {
        return command;
    }

    public void setCommand(String[] command) {
        this.command = command;
    }

    public long getMaxRSS() {
        return maxRSS;
    }

    public void setMaxRSS(long maxRSS) {
        this.maxRSS = maxRSS;
    }

    public Integer getParentId() {
        return parentId;
    }

    public void setParentId(Integer parentId) {
        this.parentId = parentId;
    }
}
