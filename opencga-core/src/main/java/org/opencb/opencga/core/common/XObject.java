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

package org.opencb.opencga.core.common;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Deprecated
public class XObject extends LinkedHashMap<String, Object> {


    private static final long serialVersionUID = -242187651119508127L;


    public XObject() {

    }

    public XObject(int size) {
        super(size);
    }

    public XObject(String key, Object value) {
        put(key, value);
    }

    public XObject(Map<String, Object> map) {
        super(map);
    }


    public boolean containsField(String key) {
        return this.containsKey(key);
    }

    public Object removeField(String key) {
        return this.remove(key);
    }


    public String getString(String field) {
        return getString(field, "");
    }

    public String getString(String field, String defaultValue) {
        if (field != null && this.containsKey(field)) {
            return (String) this.get(field);
        }
        return defaultValue;
    }


    public int getInt(String field) {
        return getInt(field, 0);
    }

    public int getInt(String field, int defaultValue) {
        if (field != null && this.containsKey(field)) {
            Object obj = this.get(field);
            switch (obj.getClass().getSimpleName()) {
                case "Integer":
                    return (Integer) obj;
                case "Double":
                    return ((Double) obj).intValue();
                case "Float":
                    return ((Float) obj).intValue();
                case "String":
                    return Integer.parseInt(String.valueOf(obj));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }

    public long getLong(String field) {
        return getLong(field, 0L);
    }

    public long getLong(String field, long defaultValue) {
        if (field != null && this.containsKey(field)) {
            Object obj = this.get(field);
            if (obj instanceof Long) {
                return (Long) obj;
            } else {
                return Long.parseLong(String.valueOf(obj));
            }
        }
        return defaultValue;
    }


    public float getFloat(String field) {
        return getFloat(field, 0.0f);
    }

    public float getFloat(String field, float defaultValue) {
        if (field != null && this.containsKey(field)) {
            Object obj = this.get(field);
            switch (obj.getClass().getSimpleName()) {
                case "Float":
                    return (Float) obj;
                case "Double":
                    return ((Double) obj).floatValue();
                case "Integer":
                    return ((Integer) obj).floatValue();
                case "String":
                    return Float.parseFloat((String) this.get(field));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    public double getDouble(String field) {
        return getDouble(field, 0.0);
    }

    public double getDouble(String field, double defaultValue) {
        if (field != null && this.containsKey(field)) {
            Object obj = this.get(field);
            switch (obj.getClass().getSimpleName()) {
                case "Double":
                    return (Double) obj;
                case "Float":
                    return ((Float) obj).doubleValue();
                case "Integer":
                    return ((Integer) obj).doubleValue();
                case "String":
                    return Double.parseDouble((String) this.get(field));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    public boolean getBoolean(String field) {
        return getBoolean(field, false);
    }

    public boolean getBoolean(String field, boolean defaultValue) {
        if (field != null && this.containsKey(field)) {
            Object obj = this.get(field);
            switch (obj.getClass().getSimpleName()) {
                case "Boolean":
                    return (Boolean) this.get(field);
                case "String":
                    return Boolean.parseBoolean((String) this.get(field));
                default:
                    return defaultValue;
            }
        }
        return defaultValue;
    }


    public List<Object> getList(String field) {
        return getList(field, null);
    }

    public List<Object> getList(String field, List<Object> defaultValue) {
        if (field != null && this.containsKey(field)) {
            return (List<Object>) this.get(field);
        }
        return defaultValue;
    }


    public Map<String, Object> getMap(String field) {
        return getMap(field, null);
    }

    public Map<String, Object> getMap(String field, Map<String, Object> defaultValue) {
        if (field != null && this.containsKey(field)) {
            return (Map<String, Object>) this.get(field);
        }
        return defaultValue;
    }


    //    @Override
    //    public String toString() {
    //        return this.toJsonByGson();
    //    }

}
