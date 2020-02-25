package org.opencb.opencga.analysis.cohort;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.annotations.TsvAnnotationLoader;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = CohortTsvAnnotationLoader.ID, resource = Enums.Resource.COHORT, type = Tool.Type.OPERATION,
        description = "Load annotations from TSV file.")
public class CohortTsvAnnotationLoader extends TsvAnnotationLoader {
    public final static String ID = "cohort-tsv-load";

    @Override
    public int count(Query query) throws CatalogException {
        return catalogManager.getCohortManager().count(study, query, token).getNumResults();
    }

    @Override
    public void addAnnotationSet(String entryId, AnnotationSet annotationSet, QueryOptions options) throws CatalogException {
        catalogManager.getCohortManager().addAnnotationSet(study, entryId, annotationSet, options, token);
    }
}
