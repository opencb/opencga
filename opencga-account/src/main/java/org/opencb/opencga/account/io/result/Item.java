package org.opencb.opencga.account.io.result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Item {

    private String name;
    private String value;
    private String title;
    private TYPE type;
    private List<String> tags;
    private Map<String, String> style;
    private String group;
    private String context;

    public enum TYPE {FILE, IMAGE, LINK, TEXT, MESSAGE, HTML, DATA}

    ;

    public Item(String name, String value, String title, TYPE type) {
        this(name, value, title, type, new ArrayList<String>(2), new HashMap<String, String>(2), "", "");
    }

    public Item(String name, String value, String title, TYPE type, String group) {
        this(name, value, title, type, new ArrayList<String>(2), new HashMap<String, String>(2), group, "");
    }

    public Item(String name, String value, String title, TYPE type, String group, String context) {
        this(name, value, title, type, new ArrayList<String>(2), new HashMap<String, String>(2), group, context);
    }

    public Item(String name, String value, String title, TYPE type, List<String> tags) {
        this(name, value, title, type, tags, new HashMap<String, String>(2), "", "");
    }

    public Item(String name, String value, String title, TYPE type, List<String> tags, Map<String, String> style) {
        this(name, value, title, type, tags, style, "", "");
    }

    public Item(String name, String value, String title, TYPE type, List<String> tags, Map<String, String> style, String group) {
        this(name, value, title, type, tags, style, group, "");
    }

    public Item(String name, String value, String title, TYPE type, List<String> tags, Map<String, String> style, String group, String context) {
        this.name = name;
        this.value = value;
        this.title = title;
        this.type = type;
        this.tags = tags;
        this.style = style;
        this.group = group;
        this.context = context;
    }

    public void addTag(String tag) {
        tags.add(tag);
    }

    public void addStyle(String key, String value) {
        this.style.put(key, value);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append("\t").append(value).append("\t").append(title).append("\t").append(getType()).append("\t").append(tags.toString()).append("\t").append(style.toString()).append("\t").append(group).append("\t").append(context);
        return sb.toString();
    }

    public String toXml() {
        StringBuilder sb = new StringBuilder();
        sb.append("<item toolName=\"").append(name).append("\" title=\"").append(title).append("\" type=\"").append(getType()).append("\" tags=\"");
        if (tags != null && tags.size() > 0) {
            for (int i = 0; i < tags.size() - 1; i++) {
                sb.append(tags.get(i)).append(",");
            }
            sb.append(tags.get(getTags().size() - 1));
        }
        sb.append("\" group=\"").append(group).append("\" context=\"").append(context).append("\">").append("\n");
        sb.append("\t").append(value);
        sb.append("</item>\n");

        return sb.toString();
    }


    /**
     * @return the toolName
     */
    public String getName() {
        return name;
    }

    /**
     * @param toolName the toolName to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return the group
     */
    public String getGroup() {
        return group;
    }

    /**
     * @param type the type to set
     */
    public void setType(TYPE type) {
        this.type = type;
    }

    /**
     * @return the type
     */
    public TYPE getType() {
        return type;
    }

    /**
     * @param group the group to set
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * @param tags the tags to set
     */
    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    /**
     * @return the tags
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * @param style the style to set
     */
    public void setStyle(HashMap<String, String> style) {
        this.style = style;
    }

    /**
     * @return the style
     */
    public Map<String, String> getStyle() {
        return style;
    }

    /**
     * @return the context
     */
    public String getContext() {
        return context;
    }

    /**
     * @param context the context to set
     */
    public void setContext(String context) {
        this.context = context;
    }

    /**
     * @param style the style to set
     */
    public void setStyle(Map<String, String> style) {
        this.style = style;
    }


}
