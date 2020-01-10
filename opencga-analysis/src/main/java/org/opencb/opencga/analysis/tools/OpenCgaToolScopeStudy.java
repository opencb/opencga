package org.opencb.opencga.analysis.tools;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;

public abstract class OpenCgaToolScopeStudy extends OpenCgaTool {

    protected final String getStudyFqn() throws CatalogException {
        String userId = getCatalogManager().getUserManager().getUserId(getToken());
        return getCatalogManager().getStudyManager().resolveId(getParams().getString(ParamConstants.STUDY_PARAM), userId).getFqn();
    }

}
