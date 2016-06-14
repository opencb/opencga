package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;

import java.util.Collection;

/**
 * Created on 12/05/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AbstractDocumentConverter {

    //Utils
    protected final Document putNotNull(Document document, String key, Object obj) {
        if (obj != null) {
            document.put(key, obj);
        }
        return document;
    }

    protected final Document putNotNull(Document document, String key, Collection obj) {
        if (obj != null && !obj.isEmpty()) {
            document.put(key, obj);
        }
        return document;
    }

    protected final Document putNotNull(Document document, String key, String obj) {
        if (obj != null && !obj.isEmpty()) {
            document.put(key, obj);
        }
        return document;
    }

    protected final Document putNotNull(Document document, String key, Integer obj) {
        if (obj != null && obj != 0) {
            document.put(key, obj);
        }
        return document;
    }

    protected final Document putNotDefault(Document document, String key, String obj, Object defaultValue) {
        if (obj != null && !obj.isEmpty() && !obj.equals(defaultValue)) {
            document.put(key, obj);
        }
        return document;
    }

    protected final String getDefault(Document object, String key, String defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            return o.toString();
        } else {
            return defaultValue;
        }
    }

    protected final <T> T getDefault(Document object, String key, T defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            return (T) o;
        } else {
            return defaultValue;
        }
    }

    protected final int getDefault(Document object, String key, int defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Integer) {
                return (Integer) o;
            } else {
                try {
                    return Integer.parseInt(o.toString());
                } catch (Exception e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

    protected final double getDefault(Document object, String key, double defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Double) {
                return (Double) o;
            } else {
                try {
                    return Double.parseDouble(o.toString());
                } catch (Exception e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

}
