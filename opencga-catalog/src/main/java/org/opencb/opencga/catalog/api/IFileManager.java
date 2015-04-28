package org.opencb.opencga.catalog.api;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.File;

import java.util.List;
import java.util.Map;

/**
* @author Jacobo Coll <jacobo167@gmail.com>
*/
public interface IFileManager extends ResourceManager<File, Integer> {
    public String  getUserId(int fileId) throws CatalogException;
    public Integer getProjectId(int fileId) throws CatalogException;
    public Integer getStudyId(int fileId) throws CatalogException;
    public Integer getFileId(String fileId) throws CatalogException;
    public QueryResult<File> create(int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                    String ownerId, String creationDate, String description, File.Status status,
                                    long diskUsage, int experimentId, List<Integer> sampleIds, int jobId,
                                    Map<String, Object> stats, Map<String, Object> attributes,
                                    boolean parents) throws CatalogException;
}
