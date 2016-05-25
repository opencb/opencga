/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.indices;

import org.opencb.opencga.core.common.XObject;

import java.util.Set;

public class DefaultParser {

    private String fieldSeparator;
    private XObject columns;


    public DefaultParser(XObject indices) {
        this(indices, "\t");
    }

    public DefaultParser(XObject columns, String fieldSeparator) {
        this.columns = columns;
        this.fieldSeparator = fieldSeparator;
    }

    public XObject parse(String record) {
        XObject obj = new XObject();
        if (record != null) {
            String[] fields = record.split(fieldSeparator, -1);
            Set<String> names = columns.keySet();
            for (String colName : names) {
                int colIndex = columns.getInt(colName);
                if (colIndex >= 0 && columns.getInt(colName) < fields.length) {
                    obj.put(colName, fields[columns.getInt(colName)]);
                }
            }
        }
        return obj;
    }
}
