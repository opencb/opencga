package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalAnalysisConverter;
import com.zettagenomics.opencga.enterprise.cvdb.converters.ClinicalInterpretationConverter;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalAnalysisSearch;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalInterpretationSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SolrSchemaSizeTest {

    @Test
    public void test() throws CvdbException, IOException {
        String studyId = "study-1";
        List<String> viewers = new ArrayList<>(Arrays.asList("user1", "user2"));

        // Converters
        ClinicalAnalysisConverter caConverter = new ClinicalAnalysisConverter();
        ClinicalInterpretationConverter ciConverter = new ClinicalInterpretationConverter();

        Path folder = Paths.get("/home/jtarraga/data/reanalysis/cipapi_grch38.opencga.models/");
        int caNumTotal = 0;
        double caTotalKbMin = 0;
        double caTotalKbMedium = 0;
        double caTotalKbMax = 0;
        int ciNumTotal = 0;
        double ciTotalKbMin = 0;
        double ciTotalKbMedium = 0;
        double ciTotalKbMax = 0;
        int ciSecNumTotal = 0;
        double ciSecTotalKbMin = 0;
        double ciSecTotalKbMedium = 0;
        double ciSecTotalKbMax = 0;
        for (File file : folder.toFile().listFiles()) {
            if (file.toString().endsWith("json")) {
                ClinicalAnalysis ca = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class).readValue(file);
                ClinicalAnalysisSearch caSearch = caConverter.toClinicalAnalysisSearch(ca, studyId, viewers);
                System.out.println((++caNumTotal) + " Clinical analyis " + ca.getId());
                System.out.printf("\t- min. JSON length   = %.2f KB\n", (caSearch.getMinJson().length() / 1000.0));
                caTotalKbMin += (caSearch.getMinJson().length() / 1000.0);
                System.out.printf("\t- medium JSON length = %.2f KB\n", (caSearch.getMediumJson().length() / 1000.0));
                caTotalKbMedium += (caSearch.getMediumJson().length() / 1000.0);
                System.out.printf("\t- max. JSON length   = %.2f KB\n", (caSearch.getMaxJson().length() / 1000.0));
                caTotalKbMax += (caSearch.getMaxJson().length() / 1000.0);

                // Primary interpretations
                if (ca.getInterpretation() != null) {
                    ClinicalInterpretationSearch ciSearch = ciConverter.toInterpretationSearch (ca.getInterpretation(), true, studyId,
                            viewers);
                    ciTotalKbMin += (ciSearch.getMinJson().length() / 1000.0);
                    ciTotalKbMedium += (ciSearch.getMediumJson().length() / 1000.0);
                    ciTotalKbMax += (ciSearch.getMaxJson().length() / 1000.0);
                    ++ciNumTotal;
                }

                // Secondary interpretations
                if (CollectionUtils.isNotEmpty(ca.getSecondaryInterpretations())) {
                    for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
                        ClinicalInterpretationSearch ciSearch = ciConverter.toInterpretationSearch (secondaryInterpretation, false, studyId,
                                viewers);
                        ciSecTotalKbMin += (ciSearch.getMinJson().length() / 1000.0);
                        ciSecTotalKbMedium += (ciSearch.getMediumJson().length() / 1000.0);
                        ciSecTotalKbMax += (ciSearch.getMaxJson().length() / 1000.0);
                        ++ciSecNumTotal;
                    }
                }
            }

            if (caNumTotal == 20) {
                break;
            }
        }

        System.out.println("\n\n\n");
        System.out.println("Average over " + caNumTotal + " clinical analyses");
        System.out.printf("\t- min. JSON length   = %.2f KB\n", (caTotalKbMin / caNumTotal));
        System.out.printf("\t- medium JSON length = %.2f KB\n", (caTotalKbMedium / caNumTotal));
        System.out.printf("\t- max. JSON length   = %.2f KB\n", (caTotalKbMax / caNumTotal));

//        System.out.println("Average over " + ciNumTotal + " primary interpretations");
//        System.out.printf("\t- min. JSON length   = %.2f KB\n", (ciTotalKbMin / ciNumTotal));
//        System.out.printf("\t- medium JSON length = %.2f KB\n", (ciTotalKbMedium / ciNumTotal));
//        System.out.printf("\t- max. JSON length   = %.2f KB\n", (ciTotalKbMax / ciNumTotal));
//
//        System.out.println("Average over " + ciSecNumTotal + " secondary interpretations");
//        System.out.printf("\t- min. JSON length   = %.2f KB\n", (ciSecTotalKbMin / ciSecNumTotal));
//        System.out.printf("\t- medium JSON length = %.2f KB\n", (ciSecTotalKbMedium / ciSecNumTotal));
//        System.out.printf("\t- max. JSON length   = %.2f KB\n", (ciSecTotalKbMax / ciSecNumTotal));
    }


}
