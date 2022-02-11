package org.opencb.opencga.core.models.study.configuration;

import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.List;

public class InterpretationVariantCallerConfiguration {

    private String id;
    private boolean somatic;
    private List<VariantType> types;
    private List<String> columns;
    private List<DataFilter> dataFilters;

    public InterpretationVariantCallerConfiguration() {
    }

    public InterpretationVariantCallerConfiguration(String id, boolean somatic, List<VariantType> types, List<String> columns,
                                                    List<DataFilter> dataFilters) {
        this.id = id;
        this.somatic = somatic;
        this.types = types;
        this.columns = columns;
        this.dataFilters = dataFilters;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationVariantCallerConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", somatic=").append(somatic);
        sb.append(", types=").append(types);
        sb.append(", columns=").append(columns);
        sb.append(", dataFilters=").append(dataFilters);
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

    public boolean isSomatic() {
        return somatic;
    }

    public InterpretationVariantCallerConfiguration setSomatic(boolean somatic) {
        this.somatic = somatic;
        return this;
    }

    public List<VariantType> getTypes() {
        return types;
    }

    public InterpretationVariantCallerConfiguration setTypes(List<VariantType> types) {
        this.types = types;
        return this;
    }

    public List<String> getColumns() {
        return columns;
    }

    public InterpretationVariantCallerConfiguration setColumns(List<String> columns) {
        this.columns = columns;
        return this;
    }

    public List<DataFilter> getDataFilters() {
        return dataFilters;
    }

    public InterpretationVariantCallerConfiguration setDataFilters(List<DataFilter> dataFilters) {
        this.dataFilters = dataFilters;
        return this;
    }

    public static class DataFilter {

        private String id;
        private String name;
        private Source source;
        private DataType type;
        private String defaultValue;
        private List<String> allowedValues;

        public enum Source {
            FILE,
            SAMPLE
        }

        public DataFilter() {
        }

        public DataFilter(String id, String name, Source source, DataType type) {
            this.name = name;
            this.id = id;
            this.source = source;
            this.type = type;
        }

        public DataFilter(String id, String name, Source source, DataType type, String defaultValue) {
            this.name = name;
            this.id = id;
            this.type = type;
            this.source = source;
            this.defaultValue = defaultValue;
        }

        public DataFilter(String id, String name, Source source, DataType type, String defaultValue, List<String> allowedValues) {
            this.name = name;
            this.id = id;
            this.type = type;
            this.source = source;
            this.defaultValue = defaultValue;
            this.allowedValues = allowedValues;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DataFilter{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", source=").append(source);
            sb.append(", type=").append(type);
            sb.append(", defaultValue='").append(defaultValue).append('\'');
            sb.append(", allowedValues=").append(allowedValues);
            sb.append('}');
            return sb.toString();
        }

        public String getName() {
            return name;
        }

        public DataFilter setName(String name) {
            this.name = name;
            return this;
        }

        public String getId() {
            return id;
        }

        public DataFilter setId(String id) {
            this.id = id;
            return this;
        }

        public Source getSource() {
            return source;
        }

        public DataFilter setSource(Source source) {
            this.source = source;
            return this;
        }

        public DataType getType() {
            return type;
        }

        public DataFilter setType(DataType type) {
            this.type = type;
            return this;
        }

        public DataFilter setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public List<String> getAllowedValues() {
            return allowedValues;
        }

        public DataFilter setAllowedValues(List<String> allowedValues) {
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
