package org.opencb.opencga.account.beans;

public class File {
    private String virtName;
    private String realName;
    private String path;

    public File() {
        this.virtName = "";
        this.realName = "";
        this.path = "";
    }

    public File(String virtName, String realName, String path) {
        this.virtName = virtName;
        this.realName = realName;
        this.path = path;
    }

    public String getVirtName() {
        return virtName;
    }

    public void setVirtName(String virtName) {
        this.virtName = virtName;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }


}
