package org.brutusin.wava.input;

public class GroupInput {

    private String groupName;
    private int priority;
    private int timetoIdleSeconds;
    private boolean delete;
    private boolean list;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getTimetoIdleSeconds() {
        return timetoIdleSeconds;
    }

    public void setTimetoIdleSeconds(int timetoIdleSeconds) {
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