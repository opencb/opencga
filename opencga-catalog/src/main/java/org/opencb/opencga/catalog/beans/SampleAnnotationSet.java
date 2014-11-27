package org.opencb.opencga.catalog.beans;

import java.util.Map;
import java.util.Set;

/**
 * Created by imedina on 25/11/14.
 */
public class SampleAnnotationSet {

    private String name;
    private Set<SampleAnnotationEntry> values;
    private String date;

    private Map<String, Object> attributes;


    public SampleAnnotationSet() {
    }

    public SampleAnnotationSet(String name, Set<SampleAnnotationEntry> values) {
        this.name = name;
        this.values = values;
    }

    public SampleAnnotationSet(String name, Set<SampleAnnotationEntry> values, String date) {
        this.name = name;
        this.values = values;
        this.date = date;
    }

    public SampleAnnotationSet(String name, Set<SampleAnnotationEntry> values, String date, Map<String, Object> attributes) {
        this.name = name;
        this.values = values;
        this.date = date;
        this.attributes = attributes;
    }

    public static class SampleAnnotationEntry {
        private String id;
        private Object value;

        public SampleAnnotationEntry() {
        }

        public SampleAnnotationEntry(String id, Object value) {
            this.id = id;
            this.value = value;
        }

        @Override
        public String toString() {
            return "SampleAnnotationEntry{" +
                    "id='" + id + '\'' +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SampleAnnotationEntry)) return false;

            SampleAnnotationEntry that = (SampleAnnotationEntry) o;

            if (!id.equals(that.id)) return false;
            if (!value.equals(that.value)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

    @Override
    public String toString() {
        return "SampleAnnotationSet{" +
                "name='" + name + '\'' +
                ", values=" + values +
                ", date='" + date + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<SampleAnnotationEntry> getValues() {
        return values;
    }

    public void setValues(Set<SampleAnnotationEntry> values) {
        this.values = values;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
