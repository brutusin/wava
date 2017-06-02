package org.brutusin.wava.input;

import java.io.File;

public class GroupInput extends Input {

    private String groupName;
    private Integer priority;
    private Integer timetoIdleSeconds;
    private boolean delete;
    private boolean list;
    private boolean noHeaders;
    private File statsDirectory;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Integer getTimetoIdleSeconds() {
        return timetoIdleSeconds;
    }

    public void setTimetoIdleSeconds(Integer timetoIdleSeconds) {
        this.timetoIdleSeconds = timetoIdleSeconds;
    }

    public boolean isDelete() {
        return delete;
    }

    public boolean isNoHeaders() {
        return noHeaders;
    }

    public void setNoHeaders(boolean noHeaders) {
        this.noHeaders = noHeaders;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public boolean isList() {
        return list;
    }

    public void setList(boolean list) {
        this.list = list;
    }

    public File getStatsDirectory() {
        return statsDirectory;
    }

    public void setStatsDirectory(File statsDirectory) {
        this.statsDirectory = statsDirectory;
    }
}