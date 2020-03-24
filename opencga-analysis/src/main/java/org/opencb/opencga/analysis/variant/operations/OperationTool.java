package org.opencb.opencga.analysis.variant.operations;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.File;

import java.nio.file.Path;

public abstract class OperationTool extends OpenCgaToolScopeStudy {

    public static final String KEEP_INTERMEDIATE_FILES = "keepIntermediateFiles";

    protected Boolean keepIntermediateFiles;

    @Override
    protected void check() throws Exception {
        super.check();
        if (keepIntermediateFiles == null) {
            keepIntermediateFiles = params.getBoolean(KEEP_INTERMEDIATE_FILES);
        }
    }

    public OperationTool setKeepIntermediateFiles(Boolean keepIntermediateFiles) {
        this.keepIntermediateFiles = keepIntermediateFiles;
        return this;
    }

    protected Path getOutDir(boolean keepIntermediateFiles) {
        if (keepIntermediateFiles) {
            return getOutDir();
        } else {
            return getScratchDir();
        }
    }

    protected final String getProjectFqn() throws CatalogException {
        return catalogManager.getProjectManager().get(params.getString(ParamConstants.PROJECT_PARAM),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.FQN.key()), token).first().getFqn();
    }

    private static boolean isVcfFormat(File.Format format) {
        return format.equals(File.Format.VCF) || format.equals(File.Format.GVCF) || format.equals(File.Format.BCF);
    }

}