package org.opencb.opencga.core.results;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 09/03/17.
 */
public class FacetedQueryResultItem {
    Map<String, Field> fields;
    Map<String, Range> ranges;

    public class Field {
        private String name;
        private long totalCount;
        private Map<String, Count> values;

        public Field() {
            this("", 0, new LinkedHashMap<>());
        }

        public Field(String name, long totalCount, Map<String, Count> values) {
            this.name = name;
            this.totalCount = totalCount;
            this.values = values;
        }
/*
        public String toString(String indent) {
            StringBuilder sb = new StringBuilder();
            sb.append(indent).append("name: ").append(name).append("\n");
            sb.append(indent).append("total count: ").append(totalCount).append("\n");
            sb.append(indent).append("values: ").append("\n");
            for (Count count: values.values()) {
                sb.append(count.toString(indent));
            }
            return sb.toString();
        }
*/
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTotalCount() {
            return totalCount;
        }

        public void setTotalCount(long totalCount) {
            this.totalCount = totalCount;
        }

        public Map<String, Count> getValues() {
            return values;
        }

        public void setValues(Map<String, Count> values) {
            this.values = values;
        }
    }

    public class Count {
        private String name;
        private String value;
        private long count;
        private Field nestedField;

        public Count(String name, String value, long count, Field nestedField) {
            this.name = name;
            this.value = value;
            this.count = count;
            this.nestedField = nestedField;
        }
/*
        public String toString(String indent) {
            StringBuilder sb = new StringBuilder();
            sb.append(indent).append("- name: ").append(name).append("\n");
            sb.append(indent).append("- value: ").append(value).append("\n");
            sb.append(indent).append("- count: ").append(count).append("\n");
            if (nestedField != null) {
                sb.append(indent).append("- nestedField: ").append("\n");
                sb.append(indent + "\t");
            } else {

            }
            sb.append(indent).append("values: ").append("\n");
            for (Count count: values.values()) {
                sb.append(count.toString(indent));
            }
            return sb.toString();
        }
*/
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public Field getNestedField() {
            return nestedField;
        }

        public void setNestedField(Field nestedField) {
            this.nestedField = nestedField;
        }
    }

    public class Range {
        private String name;
        private double start;
        private double end;
        private double gap;
        private long totalCount;
        private List<Long> counts;

        public Range() {
            this("", 0.0, 0.0, 0.0, 0, new ArrayList<>());
        }

        public Range(String name, double start, double end, double gap,
                     long totalCount, List<Long> counts) {
            this.name = name;
            this.start = start;
            this.end = end;
            this.gap = gap;
            this.totalCount = totalCount;
            this.counts = counts;
        }
/*
        public String toString(String indent) {
            StringBuilder sb = new StringBuilder();
            return sb.toString();
        }
*/
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getStart() {
            return start;
        }

        public void setStart(double start) {
            this.start = start;
        }

        public double getEnd() {
            return end;
        }

        public void setEnd(double end) {
            this.end = end;
        }

        public double getGap() {
            return gap;
        }

        public void setGap(double gap) {
            this.gap = gap;
        }

        public long getTotalCount() {
            return totalCount;
        }

        public void setGap(long totalCount) {
            this.totalCount = totalCount;
        }

        public List<Long> getCounts() {
            return counts;
        }

        public void setCounts(List<Long> counts) {
            this.counts = counts;
        }
    }

    public FacetedQueryResultItem() {
        this(new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    public FacetedQueryResultItem(Map<String, FacetedQueryResultItem.Field> fields,
                                  Map<String, FacetedQueryResultItem.Range> ranges) {
        this.fields = fields;
        this.ranges = ranges;
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public void setFields(Map<String, Field> fields) {
        this.fields = fields;
    }

    public Map<String, Range> getRanges() {
        return ranges;
    }

    public void setRanges(Map<String, Range> ranges) {
        this.ranges = ranges;
    }

    /*
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("fields: ").append("\n");
        if (this.fields.size() > 0) {
            for (Field field: fields.values()) {
                sb.append(field.toString("\t"));
            }
        } else {
            sb.append("\tNo fields.");
        }

        sb.append("ranges: ").append("\n");
        if (this.ranges.size() > 0) {
            for (Range range: ranges.values()) {
                sb.append(range.toString("\t"));
            }
        } else {
            sb.append("\tNo ranges.");
        }

        return sb.toString();
    }

    /*
    public String toString(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append("fields: ").append(field).append("\n");
        sb.append(indent).append("value: ").append(value).append("\n");
        sb.append(indent).append("count: ").append(count).append("\n");
        if (items != null && items.size() > 0) {
            sb.append(indent).append("items:\n");
            int i = 0;
            for (FacetedQueryResultItem item: items) {
                sb.append(indent).append(i++).append(":\n");
                sb.append(item.toString(indent + "\t"));
            }
        }
        return sb.toString();
    }
    */
}
