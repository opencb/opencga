/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;

import java.util.Collection;
import java.util.List;

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

    protected boolean areAllEmpty(Object... objects) {
        for (Object object : objects) {
            if (!(object == null
                    || ((object instanceof Collection) && ((Collection) object).isEmpty())
                    || ((object instanceof String) && ((String) object).isEmpty())
                    || ((object instanceof Number) && ((Number) object).doubleValue() == 0))) {
                return false;
            }
        }
        return true;
    }

    protected final String getDefault(Document object, String key, String defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            return o.toString();
        } else {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    protected final <T> T getDefault(Document object, String key, T defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            try {
                return (T) o;
            } catch (ClassCastException e) {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    protected final int getDefault(Document object, String key, int defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Number) {
                return ((Number) o).intValue();
            } else {
                try {
                    return Integer.parseInt(o.toString());
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

    protected final Float getDefault(Document object, String key, Float defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Number) {
                return ((Number) o).floatValue();
            } else {
                try {
                    return Float.parseFloat(o.toString());
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

    protected final Double getDefault(Document object, String key, Double defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Number) {
                return ((Number) o).doubleValue();
            } else {
                try {
                    return Double.parseDouble(o.toString());
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> List<T> getList(Document document, String key) {
        return (List<T>) document.get(key, List.class);
    }
}
