package org.opencb.opencga.core.models.study.configuration;

import java.util.List;

public class InterpretationVariantCallerConfiguration {

    private String id;
    private List<String> columns;
    private List<FileDataFilter> fileDataFilters;

    public InterpretationVariantCallerConfiguration() {
    }

    public InterpretationVariantCallerConfiguration(String id, List<String> columns, List<FileDataFilter> fileDataFilters) {
        this.id = id;
        this.columns = columns;
        this.fileDataFilters = fileDataFilters;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationVariantCallerConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", columns=").append(columns);
        sb.append(", fileDataFilters=").append(fileDataFilters);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public InterpretationVariantCallerConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getColumns() {
        return columns;
    }

    public InterpretationVariantCallerConfiguration setColumns(List<String> columns) {
        this.columns = columns;
        return this;
    }

    public List<FileDataFilter> getFileDataFilters() {
        return fileDataFilters;
    }

    public InterpretationVariantCallerConfiguration setFileDataFilters(List<FileDataFilter> fileDataFilters) {
        this.fileDataFilters = fileDataFilters;
        return this;
    }

    public static class FileDataFilter {

        private String id;
        private String name;
        private DataType type;
        private String defaultValue;
        private List<String> allowedValues;

        public FileDataFilter() {
        }

        public FileDataFilter(String id, String name, DataType type) {
            this.name = name;
            this.id = id;
            this.type = type;
        }

        public FileDataFilter(String id, String name, DataType type, String defaultValue) {
            this.name = name;
            this.id = id;
            this.type = type;
            this.defaultValue = defaultValue;
        }

        public FileDataFilter(String id, String name, DataType type, String defaultValue, List<String> allowedValues) {
            this.name = name;
            this.id = id;
            this.type = type;
            this.defaultValue = defaultValue;
            this.allowedValues = allowedValues;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FileDataFilter{");
            sb.append("field='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", type=").append(type);
            sb.append(", defaultValue=").append(defaultValue);
            sb.append(", allowedValues=").append(allowedValues);
            sb.append('}');
            return sb.toString();
        }

        public String getName() {
            return name;
        }

        public FileDataFilter setName(String name) {
            this.name = name;
            return this;
        }

        public String getId() {
            return id;
        }

        public FileDataFilter setId(String id) {
            this.id = id;
            return this;
        }

        public DataType getType() {
            return type;
        }

        public FileDataFilter setType(DataType type) {
            this.type = type;
            return this;
        }

        public FileDataFilter setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public List<String> getAllowedValues() {
            return allowedValues;
        }

        public FileDataFilter setAllowedValues(List<String> allowedValues) {
            this.allowedValues = allowedValues;
            return this;
        }
    }

    public enum DataType {
        BOOLEAN,
        STRING,
        NUMERIC,
        CATEGORICAL
    }

}
