package org.opencb.opencga.analysis.clinical;

import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalInterpretationManager;

import java.nio.file.Paths;

public abstract class OpenCgaClinicalAnalysis extends OpenCgaAnalysis {

    public final static String INCLUDE_LOW_COVERAGE_PARAM = "includeLowCoverage";
    public final static String MAX_LOW_COVERAGE_PARAM = "maxLowCoverage";
    public final static int LOW_COVERAGE_DEFAULT = 20;
    public static final int DEFAULT_COVERAGE_THRESHOLD = 20;

    protected String clinicalAnalysisId;

    protected ObjectMap options;

    protected ClinicalInterpretationManager clinicalInterpretationManager;
    protected CellBaseClient cellBaseClient;
    protected AlignmentStorageManager alignmentStorageManager;

    public OpenCgaClinicalAnalysis(String clinicalAnalysisId, String studyId, ObjectMap options, String opencgaHome, String sessionId) {
        super(studyId, opencgaHome, sessionId);

        this.clinicalAnalysisId = clinicalAnalysisId;
        this.options = options;

        this.clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager,
                StorageEngineFactory.get(storageConfiguration), Paths.get(opencgaHome + "/analysis/resources/roleInCancer.txt"),
                Paths.get(opencgaHome + "/analysis/resources/"));

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
    }
}
