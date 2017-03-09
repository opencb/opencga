package org.opencb.opencga.core.results;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jtarraga on 09/03/17.
 */
public class FacetedQueryResultItem {
    private String field;
    private String value;
    private long count;
    private List<FacetedQueryResultItem> items;

    public FacetedQueryResultItem() {
        this("", "", 0, new LinkedList<>());
    }

    public FacetedQueryResultItem(String field, String value, long count, List<FacetedQueryResultItem> items) {
        this.field = field;
        this.value = value;
        this.count = count;
        this.items = items;
    }

    public String toString(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append("field: ").append(field).append("\n");
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

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
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

    public List<FacetedQueryResultItem> getItems() {
        return items;
    }

    public void setItems(List<FacetedQueryResultItem> items) {
        this.items = items;
    }
}
