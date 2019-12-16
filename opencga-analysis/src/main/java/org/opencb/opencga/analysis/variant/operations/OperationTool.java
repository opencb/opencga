package org.opencb.opencga.analysis.variant.operations;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.File;

import java.nio.file.Path;

public abstract class OperationTool extends OpenCgaTool {

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

    protected final String getStudyFqn() throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        return catalogManager.getStudyManager().resolveId(params.getString(ParamConstants.STUDY_PARAM), userId).getFqn();
    }

    protected final String getStudyFqn(String study) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        return catalogManager.getStudyManager().resolveId(study, userId).getFqn();
    }

    public static boolean isVcfFormat(File file) {
        File.Format format = file.getFormat();
        if (isVcfFormat(format)) {
            return true;
        } else {
            // Do not trust the file format. Defect format from URI
            format = org.opencb.opencga.catalog.managers.FileUtils.detectFormat(file.getUri());
            if (isVcfFormat(format)) {
                // Overwrite temporary the format
                file.setFormat(format);
                return true;
            } else {
                return false;
            }
        }
    }

    private static boolean isVcfFormat(File.Format format) {
        return format.equals(File.Format.VCF) || format.equals(File.Format.GVCF) || format.equals(File.Format.BCF);
    }

}