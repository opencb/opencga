package org.opencb.opencga.catalog.core.db.converters;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.core.beans.Project;
import org.opencb.opencga.catalog.core.beans.Study;


import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 13/09/14.
 */
public class DBObjectToProjectConverter implements ComplexTypeConverter<Project, DBObject> {

    @Override
    public Project convertToDataModelType(DBObject object) {
        return staticToProject(object);
    }

    @Override
    public DBObject convertToStorageType(Project project) {
        throw new UnsupportedOperationException();
    }

    static public Project staticToProject(DBObject object) {

        if(object.get("projects")!= null){
            object = (DBObject) ((BasicDBList) object.get("projects")).get(0);
        }

        Project project = new Project();
        QueryOptions qo = new QueryOptions(object.toMap());

        project.setId(qo.getInt("id"));
        project.setName(qo.getString("name"));
        project.setCreationDate(qo.getString("creationDate"));
//        project.setAcl(           qo.getList("acl"));
        project.setAlias(qo.getString("alias"));
        project.setAttributes(qo.getMap("attributes"));
        project.setDescription(qo.getString("description"));
        project.setDiskUsage(qo.getInt("diskUsage"));
        project.setLastActivity(qo.getString("lastActivity"));
        project.setOrganization(qo.getString("organization"));
        project.setStatus(qo.getString("status"));

        List<DBObject> studiesObject = (List<DBObject>) object.get("studies");
        List<Study> studies = new LinkedList<>();
        if(studiesObject!=null){
            for (DBObject studyObject : studiesObject) {
                studies.add(DBObjectToStudyConverter.staticToStudy(studyObject));
            }
        }
        project.setStudies(studies);
        return project;
    }
}
