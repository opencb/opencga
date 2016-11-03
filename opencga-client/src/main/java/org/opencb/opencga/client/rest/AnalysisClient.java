package org.opencb.opencga.client.rest;

import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by pfurio on 03/11/16.
 */
// TODO: AnalysisClient should extend AbstractParentClient but don't need those mandatory generics. AbstractParentClient should be divided.
public class AnalysisClient extends AbstractParentClient<File, FileAclEntry> {

    private static final String ANALYSIS_URL = "analysis";

    protected AnalysisClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
        this.category = ANALYSIS_URL;
        this.clazz = File.class;
        this.aclClass = FileAclEntry.class;
    }

    public QueryResponse<ReadAlignment> alignmentQuery(ObjectMap params) throws CatalogException, IOException {
        return execute(ANALYSIS_URL, "alignment/query", params, GET, ReadAlignment.class);
    }

    public QueryResponse<AlignmentGlobalStats> alignmentStats(ObjectMap params) throws CatalogException, IOException {
        return execute(ANALYSIS_URL, "alignment/stats", params, GET, AlignmentGlobalStats.class);
    }

    public QueryResponse<RegionCoverage> alignmentCoverage(ObjectMap params) throws CatalogException, IOException {
        return execute(ANALYSIS_URL, "alignment/coverage", params, GET, RegionCoverage.class);
    }

}
