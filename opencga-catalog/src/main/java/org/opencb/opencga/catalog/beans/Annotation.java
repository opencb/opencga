package org.opencb.opencga.catalog.beans;

/**
* Created by jacobo on 12/12/14.
*/
public class Annotation {

    /**
     * This id must be a valid variable ID
     */
    private String id;

    private Object value;

    public Annotation() {
    }

    public Annotation(String id, Object value) {
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
        if (!(o instanceof Annotation)) return false;

        Annotation that = (Annotation) o;

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
