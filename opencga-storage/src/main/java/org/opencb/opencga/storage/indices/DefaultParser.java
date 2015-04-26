package org.opencb.opencga.storage.indices;

import org.opencb.opencga.core.common.XObject;

import java.util.Set;

public class DefaultParser {

    private String fieldSeparator;
    XObject columns;


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
