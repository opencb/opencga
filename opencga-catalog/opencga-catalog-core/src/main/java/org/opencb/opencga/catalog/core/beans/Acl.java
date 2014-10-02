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

    @Override
    public String toString() {
        return "Acl{" +
                "userId='" + userId + '\'' +
                ", read=" + read +
                ", write=" + write +
                ", execute=" + execute +
                ", delete=" + delete +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Acl)) return false;

        Acl acl = (Acl) o;

        if (delete != acl.delete) return false;
        if (execute != acl.execute) return false;
        if (read != acl.read) return false;
        if (write != acl.write) return false;
        if (!userId.equals(acl.userId)) return false;

        return true;
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
