package org.opencb.opencga.catalog.core.beans;

/**
 * Created by jacobo on 11/09/14.
 */
public class Acl {
    private String userId;
    private boolean read;
    private boolean write;
    private boolean execute;
    private boolean delete;


    public Acl() {
    }

    public Acl(String userId, boolean read, boolean write, boolean execute, boolean delete) {
        this.userId = userId;
        this.read = read;
        this.write = write;
        this.execute = execute;
        this.delete = delete;
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

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
}
