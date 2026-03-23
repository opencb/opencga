package org.opencb.opencga.clinical.cvdb;

import org.opencb.opencga.clinical.cvdb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.clinical.cvdb.converters.ClinicalInterpretationConverter;
import org.opencb.opencga.clinical.cvdb.converters.ClinicalVariantConverter;
import org.opencb.opencga.clinical.cvdb.converters.ClinicalVariantEvidenceConverter;
import org.opencb.opencga.clinical.cvdb.exceptions.CvdbException;
import org.opencb.opencga.clinical.cvdb.models.ClinicalAnalysisSearch;
import org.opencb.opencga.clinical.cvdb.models.ClinicalInterpretationSearch;
import org.opencb.opencga.clinical.cvdb.models.ClinicalVariantEvidenceSearch;
import org.opencb.opencga.clinical.cvdb.models.ClinicalVariantSearch;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClinicalInterpretationConverterTest {

    @Test
    public void testClinicalConverters() throws IOException, CvdbException {
        InputStream is = ClinicalInterpretationConverterTest.class.getClassLoader().getResourceAsStream("ca1.json.gz");
        GZIPInputStream gzipInputStream = new GZIPInputStream(is);
        ClinicalAnalysis ca = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class).readValue(gzipInputStream);

        String studyId = "study-1";
        List<String> viewers = new ArrayList<>(Arrays.asList("user1", "user2"));

        // Clinical analysis
        ClinicalAnalysisConverter caConverter = new ClinicalAnalysisConverter();
        ClinicalAnalysisSearch caSearch = caConverter.toClinicalAnalysisSearch(ca, studyId);
        assertEquals(ca.getType().name(), caSearch.getType());
        assertEquals(ca.getFamily().getId(), caSearch.getFamilyId());
        assertEquals(studyId, caSearch.getStudyId());

        // Interpretation
        Interpretation ci = ca.getInterpretation();
        ClinicalInterpretationConverter ciConverter = new ClinicalInterpretationConverter();
        ClinicalInterpretationSearch ciSearch = ciConverter.toInterpretationSearch(ci, true, studyId);
        assertEquals(ci.getId(), ciSearch.getId());
        assertTrue(ciSearch.isPrimary());
        assertEquals(ci.isLocked(), ciSearch.isLocked());
        assertEquals(studyId, ciSearch.getStudyId());


        // Clinical variant
        ClinicalVariantConverter cvConverter = new ClinicalVariantConverter(CvdbSolrEngine.getDefaultSearchIndexMetadata());
        List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(ci.getPrimaryFindings(), true,
                ci.getId(), true, ca.getId(), studyId);
        assertEquals(ci.getPrimaryFindings().size(), cvsList.size());
        assertEquals(studyId, cvsList.get(0).getStudyId());

        List<ClinicalVariant> cvList = cvConverter.toClinicalVariant(cvsList);
        assertEquals(ci.getPrimaryFindings().get(0).toStringSimple(), cvList.get(0).toStringSimple());
        assertEquals(ci.getPrimaryFindings().get(1).toStringSimple(), cvList.get(1).toStringSimple());

        // Clinical variant evidence
        ClinicalVariantEvidenceConverter cveConverter = new ClinicalVariantEvidenceConverter();
        List<ClinicalVariantEvidenceSearch> cvesList = new ArrayList<>();
        int evidenceIndex = 0;
        for (ClinicalVariantEvidence cve : ci.getPrimaryFindings().get(0).getEvidences()) {
            ClinicalVariantEvidenceSearch cves = cveConverter.toClinicalVariantEvidenceSearch(cve, evidenceIndex++,
                    ci.getPrimaryFindings().get(0).toStringSimple(), true, ci.getId(), true, ca.getId(), studyId);
            cvesList.add(cves);
        }
        assertEquals(ci.getPrimaryFindings().get(0).getEvidences().size(), cvesList.size());
        assertEquals(studyId, cvesList.get(0).getStudyId());

        List<ClinicalVariantEvidence> cveList = cveConverter.toClinicalVariantEvidence(cvesList);
        assertEquals(ci.getPrimaryFindings().get(0).getEvidences().size(), cveList.size());
    }
}