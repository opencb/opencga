package org.opencb.opencga.analysis.variant.geneticChecks;

import com.microsoft.graph.options.QueryOption;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.variant.MendelianErrorReport;

import java.nio.file.Path;
import java.util.List;

public class MendelianInconsistenciesComputation {

    public static MendelianErrorReport compute(String studyId, String familyId, Path outDir, VariantStorageManager storageManager,
                                               String token) throws ToolException {

        Query query = new Query();
//        storageManager.get()
        // TODO implement using variant query
        return null;
    }
}
