package org.opencb.opencga.catalog.core.db.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.core.beans.Study;

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

        QueryOptions qo = new QueryOptions(object.toMap());
        Study study = new Study();

        study.setId(qo.getInt("id"));
        study.setCreatorId(qo.getString("creatorId"));
        study.setStatus(qo.getString("status"));
        study.setAlias(qo.getString("alias"));
        study.setCipher(qo.getString(""));
        study.setAttributes(qo.getMap("attributes"));
        study.setStats(qo.getMap("stats"));
        study.setCreationDate(qo.getString("creationDate"));
        study.setDiskUsage(qo.getInt("diskUsage"));
        study.setName(qo.getString("name"));
        study.setType(qo.getString("type"));
//        study.setAcl       (qo.getList("acl"));
//        study.setExperiments       (qo.getList("experiments"));

        return study;
    }


    static public DBObject staticToDBObject(Study study) {
        throw new UnsupportedOperationException();
    }
}
