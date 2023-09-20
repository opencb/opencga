package org.opencb.opencga.app.cli.admin.executors.migration.storage;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ID;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.URI;

/**
 * Created on 04/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AddFilePathToStudyConfigurationMigration {

    protected static final QueryOptions QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(URI.key()));
    private final CatalogManager catalogManager;
    private final Logger logger = LoggerFactory.getLogger(AddFilePathToStudyConfigurationMigration.class);

    public AddFilePathToStudyConfigurationMigration(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public String getFilePath(String studyName, String fileName, String sessionId) throws CatalogException {
        // Search filePath in catalog
        logger.info("Register path from file = " + fileName);
        DataResult<File> queryResult = catalogManager.getFileManager().search(studyName, new Query()
                .append(ID.key(), fileName), QUERY_OPTIONS, sessionId);
        File file = null;
        if (queryResult.getResults().size() == 1) {
            file = queryResult.first();
        } else {
            for (File i : queryResult.getResults()) {
                if (InternalStatus.READY.equals(FileInternal.getVariantIndexStatusId(i.getInternal()))) {
                    if (file != null) {
                        throw new IllegalStateException("Error migrating storage. "
                                + "Unable to determine which file is indexed");
                    }
                    file = i;
                }
            }
        }
        if (file == null) {
            throw new IllegalStateException("Error migrating storage. File not found in catalog");
        }
        return file.getUri().getPath();
    }
}