package org.opencb.opencga.analysis.beans;

public class Icon {
    private String size, data;

    public Icon() {

    }

    public Icon(String size, String data) {
        this.size = size;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Icon{" +
                "size='" + size + '\'' +
                '}';
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
