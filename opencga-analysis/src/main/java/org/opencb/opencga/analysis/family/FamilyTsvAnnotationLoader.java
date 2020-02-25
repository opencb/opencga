package org.opencb.opencga.analysis.family;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.annotations.TsvAnnotationLoader;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = FamilyTsvAnnotationLoader.ID, resource = Enums.Resource.FAMILY, type = Tool.Type.OPERATION,
        description = "Load annotations from TSV file.")
public class FamilyTsvAnnotationLoader extends TsvAnnotationLoader {
    public final static String ID = "family-tsv-load";

    @Override
    public int count(Query query) throws CatalogException {
        return catalogManager.getFamilyManager().count(study, query, token).getNumResults();
    }

    @Override
    public void addAnnotationSet(String entryId, AnnotationSet annotationSet, QueryOptions options) throws CatalogException {
        catalogManager.getFamilyManager().addAnnotationSet(study, entryId, annotationSet, options, token);
    }
}
