package org.opencb.opencga.analysis.beans;

public class Acl {
    private String userId;
    private boolean read, write, execute;

    public Acl() {
    }

    public Acl(String userId, boolean read, boolean write, boolean execute) {
        this.userId = userId;
        this.read = read;
        this.write = write;
        this.execute = execute;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public boolean isRead() {
        return read;
    }

    public void setRead(boolean read) {
        this.read = read;
    }

    public boolean isWrite() {
        return write;
    }

    public void setWrite(boolean write) {
        this.write = write;
    }

    public boolean isExecute() {
        return execute;
    }

    public void setExecute(boolean execute) {
        this.execute = execute;
    }
}
