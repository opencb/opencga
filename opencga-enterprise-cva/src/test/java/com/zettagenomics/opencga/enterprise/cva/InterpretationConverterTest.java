package com.zettagenomics.opencga.enterprise.cva;

import com.zettagenomics.opencga.enterprise.cva.converters.ClinicalVariantConverter;
import com.zettagenomics.opencga.enterprise.cva.converters.ClinicalVariantEvidenceConverter;
import com.zettagenomics.opencga.enterprise.cva.converters.InterpretationConverter;
import com.zettagenomics.opencga.enterprise.cva.exceptions.CvaException;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalVariantEvidenceSearch;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalVariantSearch;
import com.zettagenomics.opencga.enterprise.cva.models.InterpretationSearch;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class InterpretationConverterTest {

    @Test
    public void test0() throws IOException, CvaException {
        InputStream is = InterpretationConverterTest.class.getClassLoader().getResourceAsStream("interpretation1.json");
        org.opencb.opencga.core.models.clinical.Interpretation interpretation = JacksonUtils.getDefaultObjectMapper().readerFor(org.opencb.opencga.core.models.clinical.Interpretation.class).readValue(is);
        System.out.println(interpretation.getClinicalAnalysisId());

        // Interpretation
        InterpretationConverter ciConverter = new InterpretationConverter();
        InterpretationSearch interpretationSearch = ciConverter.toInterpretationSearch((org.opencb.opencga.core.models.clinical.Interpretation) interpretation);
        assertEquals(interpretation.getMethod().getName(), interpretationSearch.getMethodName());

        Interpretation interpretation1 = ciConverter.toCInterpretation(interpretationSearch);
        assertEquals(interpretation.getMethod().getName(), interpretation1.getMethod().getName());

        // Clinical variant
        ClinicalVariantConverter cvConverter = new ClinicalVariantConverter();
        List<ClinicalVariantSearch> cvsList = cvConverter.toClinicalVariantSearch(interpretation.getPrimaryFindings(), true,
                interpretation.getId());
        assertEquals(interpretation.getPrimaryFindings().size(), cvsList.size());

        List<ClinicalVariant> cvList = cvConverter.toClinicalVariant(cvsList);
        assertEquals(interpretation.getPrimaryFindings().get(0).toStringSimple(), cvList.get(0).toStringSimple());
        assertEquals(interpretation.getPrimaryFindings().get(1).toStringSimple(), cvList.get(1).toStringSimple());

        // Clinical variant evidence
        ClinicalVariantEvidenceConverter cveConverter = new ClinicalVariantEvidenceConverter();
        List<ClinicalVariantEvidenceSearch> cvesList = cveConverter.toClinicalVariantEvidenceSearch(interpretation.getPrimaryFindings()
                .get(0).getEvidences(), interpretation.getPrimaryFindings().get(0).toStringSimple(), interpretation.getId());
        assertEquals(interpretation.getPrimaryFindings().get(0).getEvidences().size(), cvesList.size());

        List<ClinicalVariantEvidence> cveList = cveConverter.toClinicalVariantEvidence(cvesList);
        assertEquals(interpretation.getPrimaryFindings().get(0).getEvidences().size(), cveList.size());
    }
}