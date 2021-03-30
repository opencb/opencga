package org.opencb.opencga.storage.core.clinical.search;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class InterpretationConverterTest {

    @Test
    public void convert() throws IOException, ClinicalVariantException, URISyntaxException {
        ClinicalAnalysis ca = readClinicalAnalysys();

        Interpretation interpretation = ca.getInterpretation();
        ca.setInterpretation(null);
        String caJson = JacksonUtils.getDefaultObjectMapper().writerFor(ClinicalAnalysis.class).writeValueAsString(ca);
        interpretation.getAttributes().put("OPENCGA_CLINICAL_ANALYSIS", caJson);

        InterpretationConverter converter = new InterpretationConverter();
        List<ClinicalVariantSearchModel> clinicalVariantSearchList = converter.toClinicalVariantSearchList(interpretation);

        Interpretation interpretation1 = converter.toInterpretation(clinicalVariantSearchList);

        // Check
        interpretation = readClinicalAnalysys().getInterpretation();
        assert(interpretation.getPrimaryFindings().size() == interpretation1.getPrimaryFindings().size());
        assert(interpretation.getSecondaryFindings().size() == interpretation1.getSecondaryFindings().size());
        assert(interpretation.getPrimaryFindings().size() + interpretation.getSecondaryFindings().size() == clinicalVariantSearchList.size());

    }

    private ClinicalAnalysis readClinicalAnalysys() throws IOException, URISyntaxException {
        String inputPath = getClass().getResource("/clinical.analysis.json").toURI().getPath();

        File file = new File(inputPath);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        ClinicalAnalysis ca = mapper.readerFor(ClinicalAnalysis.class).readValue(file);

        return ca;
    }
}