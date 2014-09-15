package org.opencb.opencga.catalog.core.db.converters;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.ObjectMap;
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
        ObjectMap om = new ObjectMap(object.toMap());

        project.setId(om.getInt("id"));
        project.setName(om.getString("name"));
        project.setCreationDate(om.getString("creationDate"));
//        project.setAcl(           om.getList("acl"));
        project.setAlias(om.getString("alias"));
        project.setAttributes(om.getMap("attributes"));
        project.setDescription(om.getString("description"));
        project.setDiskUsage(om.getInt("diskUsage"));
        project.setLastActivity(om.getString("lastActivity"));
        project.setOrganization(om.getString("organization"));
        project.setStatus(om.getString("status"));

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
