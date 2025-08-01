package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalAnalysisConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalInterpretationConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalVariantEvidenceConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalAnalysisSearch;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalInterpretationSearch;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalVariantEvidenceSearch;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalVariantSearch;
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
        ClinicalAnalysisSearch caSearch = caConverter.toClinicalAnalysisSearch(ca, studyId, viewers);
        assertEquals(ca.getType().name(), caSearch.getType());
        assertEquals(ca.getFamily().getId(), caSearch.getFamilyId());
        assertEquals(studyId, caSearch.getStudyId());
        assertEquals(viewers.size(), caSearch.getViewers().size());

        // Interpretation
        Interpretation ci = ca.getInterpretation();
        ClinicalInterpretationConverter ciConverter = new ClinicalInterpretationConverter();
        ClinicalInterpretationSearch ciSearch = ciConverter.toInterpretationSearch(ci, true, studyId, viewers);
        assertEquals(ci.getId(), ciSearch.getId());
        assertTrue(ciSearch.isPrimary());
        assertEquals(ci.isLocked(), ciSearch.isLocked());
        assertEquals(studyId, ciSearch.getStudyId());
        assertEquals(viewers.size(), ciSearch.getViewers().size());


        // Clinical variant
        ClinicalVariantConverter cvConverter = new ClinicalVariantConverter(null);
        List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(ci.getPrimaryFindings(), true,
                ci.getId(), true, ca.getId(), studyId, viewers);
        assertEquals(ci.getPrimaryFindings().size(), cvsList.size());
        assertEquals(studyId, cvsList.get(0).getStudyId());
        assertEquals(viewers.size(), cvsList.get(0).getViewers().size());

        List<ClinicalVariant> cvList = cvConverter.toClinicalVariant(cvsList);
        assertEquals(ci.getPrimaryFindings().get(0).toStringSimple(), cvList.get(0).toStringSimple());
        assertEquals(ci.getPrimaryFindings().get(1).toStringSimple(), cvList.get(1).toStringSimple());

        // Clinical variant evidence
        ClinicalVariantEvidenceConverter cveConverter = new ClinicalVariantEvidenceConverter();
        List<ClinicalVariantEvidenceSearch> cvesList = new ArrayList<>();
        int evidenceIndex = 0;
        for (ClinicalVariantEvidence cve : ci.getPrimaryFindings().get(0).getEvidences()) {
            ClinicalVariantEvidenceSearch cves = cveConverter.toClinicalVariantEvidenceSearch(cve, evidenceIndex++,
                    ci.getPrimaryFindings().get(0).toStringSimple(), true, ci.getId(), true, ca.getId(), studyId, viewers);
            cvesList.add(cves);
        }
        assertEquals(ci.getPrimaryFindings().get(0).getEvidences().size(), cvesList.size());
        assertEquals(studyId, cvesList.get(0).getStudyId());
        assertEquals(viewers.size(), cvesList.get(0).getViewers().size());

        List<ClinicalVariantEvidence> cveList = cveConverter.toClinicalVariantEvidence(cvesList);
        assertEquals(ci.getPrimaryFindings().get(0).getEvidences().size(), cveList.size());
    }
}