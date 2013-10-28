package org.opencb.opencga.analysis.beans;

public class Icon {
    private String size, data;

    public Icon(String size, String data) {
        this.size = size;
        this.data = data;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
