package org.opencb.opencga.analysis.beans;

public class InputParam {
    private String name, dataType;

    public InputParam() {

    }

    public InputParam(String name, String dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return "InputParam{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}
