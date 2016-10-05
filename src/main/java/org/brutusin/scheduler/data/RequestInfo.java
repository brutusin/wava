package org.brutusin.scheduler.data;

import java.io.File;
import java.util.Map;
import org.brutusin.json.annotations.JsonProperty;

public class RequestInfo {

    @JsonProperty(description = "Group id of the request. Requests of the same group are scheduled in a FIFO way", required = true)
    private int groupId;

    @JsonProperty(description = "Command and parameters to execute")
    private String[] command;

    @JsonProperty(description = "Maximum [RSS](https://en.wikipedia.org/wiki/Resident_set_size) that the execution will require")
    private long maxRSS;
    
    @JsonProperty(description = "Environment properties")
    private Map<String,String> environment;
    
    @JsonProperty(description = "Working directory")
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
    
    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
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
}
