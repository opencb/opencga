package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.junit.Rule;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;

public class CvdbSolrEngineResource {

    protected CvdbSolrEngine cvaEngine;
    protected String projectId = "project1";
    protected boolean initialized = false;

    @Rule
    public CvaSolrExtenalResource cvaSolrExternalResource = new CvaSolrExtenalResource(false, projectId);;

    public void init() throws IOException, CvdbException {
        if (!initialized) {
            cvaEngine = cvaSolrExternalResource.configure();

            try {
                cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX));
                cvaEngine.getSolrManager().remove(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX));
                cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX));
                cvaEngine.getSolrManager().remove(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
            } catch (Exception e) {
                // Nothing to do
            }

            cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_ANALYSES_COLLECTION_SUFFIX),
                    CLINICAL_ANALYSIS_CONFIGSET);
            cvaEngine.getSolrManager().createCore(getCollectionName(projectId, INTERPRETATIONS_COLLECTION_SUFFIX), INTERPRETATION_CONFIGSET);
            cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_VARIANTS_COLLECTION_SUFFIX),
                    CLINICAL_VARIANT_CONFIGSET);
            cvaEngine.getSolrManager().createCore(getCollectionName(projectId, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
                    CLINICAL_VARIANT_EVIDENCE_CONFIGSET);

            List<String> names = Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz");
            for (String name : names) {
                InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(name);
                GZIPInputStream gzipInputStream = new GZIPInputStream(is);
                ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class).readValue(gzipInputStream);
                cvaEngine.index(clinicalAnalysis, projectId);
                System.out.println("Clinical analysis " + clinicalAnalysis.getId() + " loaded !");
                break;
            }
        }

        initialized = true;
    }
}


