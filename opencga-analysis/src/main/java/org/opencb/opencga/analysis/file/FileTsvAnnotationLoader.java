package org.opencb.opencga.analysis.file;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.annotations.TsvAnnotationLoader;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Collections;

@Tool(id = FileTsvAnnotationLoader.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = "Load annotations from TSV file.")
public class FileTsvAnnotationLoader extends TsvAnnotationLoader {
    public final static String ID = "file-tsv-load";

    @Override
    public int count(Query query) throws CatalogException {
        return catalogManager.getFileManager().count(study, query, token).getNumResults();
    }

    @Override
    public void addAnnotationSet(String entryId, AnnotationSet annotationSet, QueryOptions options) throws CatalogException {
        FileUpdateParams fileUpdateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.UpdateAction.ADD));
        catalogManager.getFileManager().update(study, entryId, fileUpdateParams, options, token);
    }
}
