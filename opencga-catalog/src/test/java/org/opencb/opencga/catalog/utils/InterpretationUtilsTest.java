package org.opencb.opencga.catalog.utils;

import org.junit.Test;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.InterpretationStats;
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.core.models.clinical.Interpretation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class InterpretationUtilsTest {

    private static final List<String> GENE_NAMES = Arrays.asList("gene1", "gene2", "gene3");
    private static final List<String> TIER_NAMES = Arrays.asList("TIER1", "TIER2", "TIER3", "NONE");

    @Test
    public void calculateStats() {
        Interpretation interpretation = new Interpretation()
                .setPrimaryFindings(Arrays.asList(
                        getClinicalVariant(1),
                        getClinicalVariant(2),
                        getClinicalVariant(3),
                        getClinicalVariant(4)
                ))
                .setSecondaryFindings(Arrays.asList(
                        getClinicalVariant(4),
                        getClinicalVariant(5),
                        getClinicalVariant(6),
                        getClinicalVariant(7),
                        getClinicalVariant(8),
                        getClinicalVariant(9)
                ));
        InterpretationStats interpretationStats = InterpretationUtils.calculateStats(interpretation);

        assertEquals(4, interpretationStats.getPrimaryFindings().getNumVariants());
        assertEquals(4, interpretationStats.getPrimaryFindings().getTierCount().size());
        for (String tierName : TIER_NAMES) {
            assertEquals(1, (int) interpretationStats.getPrimaryFindings().getTierCount().get(tierName));
        }
        for (ClinicalVariant.Status value : ClinicalVariant.Status.values()) {
            if (value == ClinicalVariant.Status.NOT_REVIEWED || value == ClinicalVariant.Status.ARTIFACT) {
                assertEquals(0, (int) interpretationStats.getPrimaryFindings().getStatusCount().get(value));
            } else {
                assertEquals(1, (int) interpretationStats.getPrimaryFindings().getStatusCount().get(value));
            }
        }
        assertEquals(3, interpretationStats.getPrimaryFindings().getGeneCount().size());
        assertEquals(1, (int) interpretationStats.getPrimaryFindings().getGeneCount().get(GENE_NAMES.get(0)));
        assertEquals(2, (int) interpretationStats.getPrimaryFindings().getGeneCount().get(GENE_NAMES.get(1)));
        assertEquals(1, (int) interpretationStats.getPrimaryFindings().getGeneCount().get(GENE_NAMES.get(2)));

        assertEquals(6, interpretationStats.getSecondaryFindings().getNumVariants());
        assertEquals(4, interpretationStats.getSecondaryFindings().getTierCount().size());
        for (int i = 0; i < TIER_NAMES.size(); i++) {
            String tierName = TIER_NAMES.get(i);
            if (i > 1) {
                assertEquals(1, (int) interpretationStats.getSecondaryFindings().getTierCount().get(tierName));
            } else {
                assertEquals(2, (int) interpretationStats.getSecondaryFindings().getTierCount().get(tierName));
            }
        }
        for (ClinicalVariant.Status value : ClinicalVariant.Status.values()) {
            if (value == ClinicalVariant.Status.REPORTED) {
                assertEquals(2, (int) interpretationStats.getSecondaryFindings().getStatusCount().get(value));
            } else if (value == ClinicalVariant.Status.ARTIFACT) {
                assertEquals(0, (int) interpretationStats.getSecondaryFindings().getStatusCount().get(value));
            } else {
                assertEquals(1, (int) interpretationStats.getSecondaryFindings().getStatusCount().get(value));
            }
        }
        assertEquals(3, interpretationStats.getSecondaryFindings().getGeneCount().size());
        assertEquals(2, (int) interpretationStats.getSecondaryFindings().getGeneCount().get(GENE_NAMES.get(0)));
        assertEquals(2, (int) interpretationStats.getSecondaryFindings().getGeneCount().get(GENE_NAMES.get(1)));
        assertEquals(2, (int) interpretationStats.getSecondaryFindings().getGeneCount().get(GENE_NAMES.get(2)));
    }

    private ClinicalVariant getClinicalVariant(int i) {
        String gene = GENE_NAMES.get(i % 3);
        String tier = TIER_NAMES.get(i % 4);
        ClinicalVariant.Status status = Arrays.asList(ClinicalVariant.Status.values()).get(i % 5);
        VariantAnnotation variantAnnotation = new VariantAnnotation();
        variantAnnotation.setConsequenceTypes(Collections.singletonList(
                new ConsequenceType(gene, gene, gene, null, null, null, null, null, null, null, null, null, null, null, null, null, null)));
        VariantAvro variantAvro = new VariantAvro("chr" + i, null, "chr" + i, 0, 10, "G", "T", "+", null, 10, VariantType.CNV, null,
                variantAnnotation);

        ClinicalVariantEvidence evidence = new ClinicalVariantEvidence()
                .setClassification(new VariantClassification().setTier(tier));
        List<ClinicalVariantEvidence> variantEvidenceList = new ArrayList<>(i);
        for (int j = 0; j < i; j++) {
            variantEvidenceList.add(evidence);
        }

        return new ClinicalVariant(variantAvro, variantEvidenceList, null, null, null, null, status, Collections.emptyList(), null);
    }
}