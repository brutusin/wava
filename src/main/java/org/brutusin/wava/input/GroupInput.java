package org.brutusin.wava.input;

public class GroupInput {

    private String groupName;
    private Integer priority;
    private Integer timetoIdleSeconds;
    private boolean delete;
     private boolean list;

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

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public boolean isList() {
        return list;
    }

    public void setList(boolean list) {
        this.list = list;
    }
}