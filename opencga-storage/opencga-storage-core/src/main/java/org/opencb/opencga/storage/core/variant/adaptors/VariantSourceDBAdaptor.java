package org.opencb.opencga.storage.core.variant.adaptors;

import java.util.List;

import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface VariantSourceDBAdaptor {

    QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options);

    QueryResult countSources();

    QueryResult getAllSources(QueryOptions options);
    
    QueryResult getAllSourcesByStudyId(String studyId, QueryOptions options);

    QueryResult getAllSourcesByStudyIds(List<String> studyIds, QueryOptions options);
    
    QueryResult getSamplesBySource(String fileId, QueryOptions options);

    QueryResult getSamplesBySources(List<String> fileIds, QueryOptions options);

    QueryResult getSourceDownloadUrlByName(String filename);

    List<QueryResult> getSourceDownloadUrlByName(List<String> filenames);

    QueryResult getSourceDownloadUrlById(String fileId, String studyId);

    QueryResult updateSourceStats(VariantSourceStats variantSourceStats, QueryOptions queryOptions);
    
    boolean close();

}
