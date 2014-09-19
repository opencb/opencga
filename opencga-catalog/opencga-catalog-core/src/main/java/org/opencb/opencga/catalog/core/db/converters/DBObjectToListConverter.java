package org.opencb.opencga.catalog.core.db.converters;

import com.mongodb.DBObject;
import org.opencb.datastore.core.ComplexTypeConverter;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 14/09/14.
 *
 * Converts nested objects with a specified ComplexTypeConverter
 * Nesting route is specified in the constructor.
 *
 * Return a list of converted objects.
 *
 * Example: Route = [o1, o2]
 * {o1:{o2:[{o3:{id:1}},{o3:{id:2}}}} -> convert({o3:{id:1}}) , convert({o3:{id:2}})
 *
 */
public class DBObjectToListConverter<T> implements ComplexTypeConverter<List<T>, DBObject> {

    private final ComplexTypeConverter<T, DBObject> converter;
    private final String[] route;

    /**
     *
     * @param converter Converter object for the ModelType .
     * @param route     Nesting route.
     */
    public DBObjectToListConverter(ComplexTypeConverter<T, DBObject> converter, String... route) {
        this.converter = converter;
        this.route = route;
    }

    @Override
    public List<T> convertToDataModelType(DBObject object) {
        List<T> list = new LinkedList<>();

        for (String name : route) {
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
