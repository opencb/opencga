package org.opencb.opencga.storage.core.clinical.search;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class InterpretationConverterTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void convert() throws IOException, ClinicalVariantException {
        ClinicalAnalysis ca = readClinicalAnalysys();

        Interpretation interpretation = ca.getInterpretation();


        InterpretationConverter converter = new InterpretationConverter();
        List<ClinicalVariantSearchModel> clinicalVariantSearchList = converter.toClinicalVariantSearchList(interpretation);

//        int i = 0;
//        for (ClinicalVariantSearchModel cvsm : clinicalVariantSearchList) {
//            System.out.println(">>>>>>>>>>>>>>>>>>>>> " + (i++));
//            System.out.println(">>>>>>>>>>>>>>>>>>>>> JUSTIFICATION");
//            for (Map.Entry<String, List<String>> entry : cvsm.getCveJustification().entrySet()) {
//                System.out.println("key = " + entry.getKey());
//                if (CollectionUtils.isNotEmpty(entry.getValue())) {
//                    for (String value : entry.getValue()) {
//                        if (StringUtils.isNotEmpty(value)) {
//                            System.out.println("\t" + value);
//                        }
//                    }
//                }
//            }
//            System.out.println(">>>>>>>>>>>>>>>>>>>>> AUX");
//            if (CollectionUtils.isNotEmpty(cvsm.getCveAux())) {
//                for (String aux : cvsm.getCveAux()) {
//                    System.out.println(aux);
//                }
//            }
//        }


        Interpretation interpretation1 = converter.toInterpretation(clinicalVariantSearchList);

        System.out.println(interpretation);
        System.out.println("=====================================================");
        System.out.println(interpretation1);
    }

    private ClinicalAnalysis readClinicalAnalysys() throws IOException {
        String inputPath = getClass().getResource("/clinical.analysis.json").toString();
        File file = new File(inputPath);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        ClinicalAnalysis ca = mapper.readerFor(ClinicalAnalysis.class).readValue(file);

        System.out.println(ca.getId());

        return ca;
    }
}