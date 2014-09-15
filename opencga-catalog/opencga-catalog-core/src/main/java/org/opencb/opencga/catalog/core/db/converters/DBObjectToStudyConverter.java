package org.opencb.opencga.catalog.core.db.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.core.beans.Study;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by jacobo on 14/09/14.
 */
public class DBObjectToStudyConverter  implements ComplexTypeConverter <Study,DBObject>{


    @Override
    public Study convertToDataModelType(DBObject object) {
        return staticToStudy(object);
    }

    @Override
    public DBObject convertToStorageType(Study object) {
        return staticToDBObject(object);
    }


    static public Study staticToStudy(DBObject object) {
        if(object.get("projects")!= null){
            object = (DBObject) ((BasicDBList) object.get("projects")).get(0);
        }
        if(object.get("studies")!= null){
            object = (DBObject) ((BasicDBList) object.get("studies")).get(0);
        }
        ObjectMap om = new ObjectMap(object.toMap());
        Study study = new Study();

        study.setId(om.getInt("id"));
        study.setCreatorId(om.getString("creatorId"));
        study.setStatus(om.getString("status"));
        study.setAlias(om.getString("alias"));
        study.setCipher(om.getString(""));
        study.setAttributes(om.getMap("attributes"));
        study.setStats(om.getMap("stats"));
        study.setCreationDate(om.getString("creationDate"));
        study.setDiskUsage(om.getInt("diskUsage"));
        study.setName(om.getString("name"));
        study.setType(om.getString("type"));
//        study.setAcl       (om.getList("acl"));
//        study.setExperiments       (om.getList("experiments"));

        return study;
    }


    static public DBObject staticToDBObject(Study study) {
        throw new UnsupportedOperationException();
    }
}
