package org.opencb.opencga.analysis.beans;


public class ExampleOption {
    private String paramName, value;

    public ExampleOption() {

    }

    public ExampleOption(String executionId, String value) {
        this.paramName = executionId;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ExampleOption{" +
                "paramName='" + paramName + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
