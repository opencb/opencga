package org.opencb.opencga.catalog.core.db.converters;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.opencb.datastore.core.ComplexTypeConverter;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 14/09/14.
 */
public class DBObjectToListConverter<T> implements ComplexTypeConverter<List<T>, DBObject> {

    private final ComplexTypeConverter<T, DBObject> converter;
    private final String[] names;

    public DBObjectToListConverter(ComplexTypeConverter<T, DBObject> converter, String... name) {
        this.converter = converter;
        this.names = name;
    }

    @Override
    public List<T> convertToDataModelType(DBObject object) {
        List<T> list = new LinkedList<>();

        for (String name : names) {
            if(object instanceof List){
                object = (DBObject) ((List) object).get(0);
            }
            Object o = object.get(name);
            if(o != null){
                object = (DBObject) o;
            }
        }
        if(object != null && object instanceof List){
            for (DBObject dbo : ((List<DBObject>) object)) {
                list.add(converter.convertToDataModelType(dbo));
            }
        }
        return list;
    }

    @Override
    public DBObject convertToStorageType(List<T> object) {
        throw new UnsupportedOperationException();
    }
}
