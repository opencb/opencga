package org.opencb.opencga.account.beans;

public class Acl {

    private String accountId;
    private String status;
    private boolean read;
    private boolean write;
    private boolean execute;

    public Acl() {
        this("", "", false, false, false);
    }

    public Acl(String accountId, String status, boolean read, boolean write, boolean execute) {
        this.accountId = accountId;
        this.status = status;
        this.read = read;
        this.write = write;
        this.execute = execute;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isWrite() {
        return write;
    }

    public void setWrite(boolean write) {
        this.write = write;
    }

    public boolean isRead() {
        return read;
    }

    public void setRead(boolean read) {
        this.read = read;
    }

    public boolean isExecute() {
        return execute;
    }

    public void setExecute(boolean execute) {
        this.execute = execute;
    }


}
