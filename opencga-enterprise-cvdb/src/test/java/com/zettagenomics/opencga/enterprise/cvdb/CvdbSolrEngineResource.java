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

public class CvdbSolrEngineResource {

    protected CvdbSolrEngine cvdbEngine;
    protected String projectId = "project1";
    protected boolean initialized = false;

    @Rule
    public CvdbSolrExtenalResource cvdbSolrExternalResource = new CvdbSolrExtenalResource(false, projectId);;

    public void init() throws IOException, CvdbException {
        if (!initialized) {
            cvdbEngine = cvdbSolrExternalResource.configure();

            try {
                cvdbEngine.getSolrManager().remove(CvdbSolrEngine.getCollectionName(projectId,
                        CvdbSolrEngine.CLINICAL_ANALYSES_COLLECTION_SUFFIX));
                cvdbEngine.getSolrManager().remove(CvdbSolrEngine.getCollectionName(projectId,
                        CvdbSolrEngine.INTERPRETATIONS_COLLECTION_SUFFIX));
                cvdbEngine.getSolrManager().remove(CvdbSolrEngine.getCollectionName(projectId,
                        CvdbSolrEngine.CLINICAL_VARIANTS_COLLECTION_SUFFIX));
                cvdbEngine.getSolrManager().remove(CvdbSolrEngine.getCollectionName(projectId,
                        CvdbSolrEngine.CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
            } catch (Exception e) {
                // Nothing to do
            }

            cvdbEngine.getSolrManager().createCore(CvdbSolrEngine.getCollectionName(projectId,
                            CvdbSolrEngine.CLINICAL_ANALYSES_COLLECTION_SUFFIX), CvdbSolrEngine.CLINICAL_ANALYSIS_CONFIGSET);
            cvdbEngine.getSolrManager().createCore(CvdbSolrEngine.getCollectionName(projectId,
                    CvdbSolrEngine.INTERPRETATIONS_COLLECTION_SUFFIX), CvdbSolrEngine.INTERPRETATION_CONFIGSET);
            cvdbEngine.getSolrManager().createCore(CvdbSolrEngine.getCollectionName(projectId,
                            CvdbSolrEngine.CLINICAL_VARIANTS_COLLECTION_SUFFIX), CvdbSolrEngine.CLINICAL_VARIANT_CONFIGSET);
            cvdbEngine.getSolrManager().createCore(CvdbSolrEngine.getCollectionName(projectId,
                            CvdbSolrEngine.CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX),
                    CvdbSolrEngine.CLINICAL_VARIANT_EVIDENCE_CONFIGSET);

            List<String> names = Arrays.asList("ca1.json.gz", "ca2.json.gz", "ca3.json.gz");
            for (String name : names) {
                InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream(name);
                GZIPInputStream gzipInputStream = new GZIPInputStream(is);
                ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class).readValue(gzipInputStream);
                cvdbEngine.index(clinicalAnalysis, projectId, true);
                System.out.println("Clinical analysis " + clinicalAnalysis.getId() + " loaded !");
                break;
            }
        }

        initialized = true;
    }
}


