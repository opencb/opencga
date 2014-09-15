package org.opencb.opencga.catalog.core.db.converters;

import com.mongodb.DBObject;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.core.beans.File;

import java.util.List;

/**
 * Created by jacobo on 14/09/14.
 */
public class DBObjectToFileConverter implements ComplexTypeConverter<File, DBObject> {

    @Override
    public File convertToDataModelType(DBObject object) {
        File file = new File();
        ObjectMap om = new ObjectMap(object.toMap());

        file.setId(om.getInt("id"));
        file.setName(om.getString("name"));
        file.setType(om.getString("type"));
        file.setFormat(om.getString("format"));
        file.setBioformat(om.getString("bioformat"));
        file.setUri(om.getString("uri"));
        file.setCreatorId(om.getString("creatorId"));
        file.setCreationDate(om.getString("creationDate"));
        file.setDescription(om.getString("description"));
        file.setStatus(om.getString("status"));
        file.setDiskUsage(om.getInt("diskUsage"));
        file.setStudyId(om.getInt("studyId"));
        file.setExperimentId(om.getInt("experimentId"));
        file.setJobId(om.getInt("jobId"));
        file.setSampleIds((List)om.getList("sampleIds"));
        file.setAttributes(om.getMap("attributes"));
        file.setStats(om.getMap("stats"));

        return file;
    }

    @Override
    public DBObject convertToStorageType(File object) {
        throw new UnsupportedOperationException();
    }
}
