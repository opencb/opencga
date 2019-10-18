package org.opencb.opencga.analysis.clinical;

import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalInterpretationManager;

import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class OpenCgaClinicalAnalysis extends OpenCgaAnalysis {

    protected String clinicalAnalysisId;
    protected ClinicalInterpretationManager clinicalInterpretationManager;

    public OpenCgaClinicalAnalysis(String clinicalAnalysisId, String studyId, Path outDir, Path openCgaHome, String sessionId) {
        super(studyId, outDir, openCgaHome, sessionId);

        this.clinicalAnalysisId = clinicalAnalysisId;
        this.clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager,
                StorageEngineFactory.get(storageConfiguration), Paths.get(openCgaHome + "/analysis/resources/roleInCancer.txt"),
                Paths.get(openCgaHome + "/analysis/resources/"));
    }
}
