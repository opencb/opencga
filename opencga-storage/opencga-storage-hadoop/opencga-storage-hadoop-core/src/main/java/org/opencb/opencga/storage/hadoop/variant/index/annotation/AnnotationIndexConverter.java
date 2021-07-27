package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.util.*;

import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.core.models.variant.VariantAnnotationConstants.*;

/**
 * Created by jacobo on 04/01/19.
 */
public class AnnotationIndexConverter {

    public static final String GNOMAD_GENOMES = "GNOMAD_GENOMES";
    public static final String K_GENOMES = "1kG_phase3";

    public static final double POP_FREQ_THRESHOLD_001 = 0.001;
    public static final Set<String> BIOTYPE_SET;
    public static final Set<String> POP_FREQ_ANY_001_SET = new HashSet<>();
    public static final Set<String> POP_FREQ_ANY_001_FILTERS = new HashSet<>();

    public static final byte PROTEIN_CODING_MASK =               (byte) (1 << 0);
    public static final byte LOF_MASK =                          (byte) (1 << 1);
    public static final byte MISSENSE_VARIANT_MASK =             (byte) (1 << 2);
    public static final byte LOFE_PROTEIN_CODING_MASK =          (byte) (1 << 3);
    public static final byte LOF_EXTENDED_MASK =                 (byte) (1 << 4);
    public static final byte POP_FREQ_ANY_001_MASK =             (byte) (1 << 5);
    public static final byte CLINICAL_MASK =                     (byte) (1 << 6);
    public static final byte INTERGENIC_MASK =                   (byte) (1 << 7);  // INTERGENIC (and maybe regulatory)

    public static final String TRANSCRIPT_FLAG_BASIC = "basic";

    static {
        BIOTYPE_SET = Collections.singleton(PROTEIN_CODING);
//        BIOTYPE_SET.add(PROTEIN_CODING);
//        BIOTYPE_SET.add(IG_C_GENE);
//        BIOTYPE_SET.add(IG_D_GENE);
//        BIOTYPE_SET.add(IG_J_GENE);
//        BIOTYPE_SET.add(IG_V_GENE);
//        BIOTYPE_SET.add(NONSENSE_MEDIATED_DECAY);
//        BIOTYPE_SET.add(NON_STOP_DECAY);
//        BIOTYPE_SET.add(TR_C_GENE);
//        BIOTYPE_SET.add(TR_D_GENE);
//        BIOTYPE_SET.add(TR_J_GENE);
//        BIOTYPE_SET.add(TR_V_GENE);

        POP_FREQ_ANY_001_SET.add("1kG_phase3:ALL");
        POP_FREQ_ANY_001_SET.add("GNOMAD_GENOMES:ALL");

        for (String s : POP_FREQ_ANY_001_SET) {
            POP_FREQ_ANY_001_FILTERS.add(s + "<" + POP_FREQ_THRESHOLD_001);
        }
    }

    private final SampleIndexSchema schema;

    public AnnotationIndexConverter(SampleIndexSchema schema) {
        this.schema = schema;
    }

    public AnnotationIndexEntry convert(VariantAnnotation variantAnnotation) {
        if (variantAnnotation == null) {
            return AnnotationIndexEntry.empty(schema);
        }
        byte b = 0;
        BitBuffer ctIndex = new BitBuffer(schema.getCtIndex().getBitsLength());
        BitBuffer btIndex = new BitBuffer(schema.getBiotypeIndex().getBitsLength());
        BitBuffer clinicalIndex = new BitBuffer(schema.getClinicalIndexSchema().getBitsLength());
        BitBuffer popFreq = new BitBuffer(schema.getPopFreqIndex().getBitsLength());

        boolean intergenic = false;
        boolean clinical = false;

        List<Pair<String, String>> ctBtPair = new LinkedList<>();
        AnnotationIndexEntry.CtBtCombination ctBtCombination = null;
        if (variantAnnotation.getConsequenceTypes() != null) {
            Set<String> biotypes = new HashSet<>();
            Set<String> cts = new HashSet<>();
            for (ConsequenceType ct : variantAnnotation.getConsequenceTypes()) {
                String biotype = ct.getBiotype();
                if (BIOTYPE_SET.contains(biotype)) {
                    b |= PROTEIN_CODING_MASK;
                }
                biotypes.add(biotype);

                boolean proteinCoding = PROTEIN_CODING.equals(biotype);
                for (SequenceOntologyTerm sequenceOntologyTerm : ct.getSequenceOntologyTerms()) {
                    String soName = sequenceOntologyTerm.getName();
                    if (!intergenic && INTERGENIC_VARIANT.equals(soName)) {
                        intergenic = true;
                    }

                    cts.add(soName);
                    ctBtPair.add(Pair.of(soName, biotype));

                    if (VariantQueryUtils.LOF_SET.contains(soName)) {
                        b |= LOF_MASK;
                        b |= LOF_EXTENDED_MASK;
                        if (proteinCoding) {
                            b |= LOFE_PROTEIN_CODING_MASK;
                        }
                    } else if (MISSENSE_VARIANT.equals(soName)) {
                        b |= MISSENSE_VARIANT_MASK;
                        b |= LOF_EXTENDED_MASK;
                        if (proteinCoding) {
                            b |= LOFE_PROTEIN_CODING_MASK;
                        }
                    }
                }
            }
            schema.getBiotypeIndex().getField().write(new ArrayList<>(biotypes), btIndex);
            schema.getCtIndex().getField().write(new ArrayList<>(cts), ctIndex);
            ctBtCombination = schema.getCtBtIndex().getField().getCtBtCombination(ctBtPair, ctIndex, btIndex);
        }
        if (intergenic) {
            b |= INTERGENIC_MASK;
        }


        // By default, population frequency is 0.
        double minFreq = 0;
        if (variantAnnotation.getPopulationFrequencies() != null) {
            double gnomadFreq = 0;
            double kgenomesFreq = 0;
            for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                addPopFreqIndex(popFreq, populationFrequency);
                if (populationFrequency.getPopulation().equals(DEFAULT_COHORT)) {
                    if (populationFrequency.getStudy().equals(GNOMAD_GENOMES)) {
                        gnomadFreq = populationFrequency.getAltAlleleFreq();
                    } else if (populationFrequency.getStudy().equals(K_GENOMES)) {
                        kgenomesFreq = populationFrequency.getAltAlleleFreq();
                    }
                }
            }
            minFreq = Math.min(gnomadFreq, kgenomesFreq);
        }
        if (minFreq < POP_FREQ_THRESHOLD_001) {
            b |= POP_FREQ_ANY_001_MASK;
        }

        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            b |= CLINICAL_MASK;
            clinical = true;
            List<String> combinations = VariantQueryUtils.buildClinicalCombinations(variantAnnotation);
            Set<String> source = new HashSet<>();
//            for (EvidenceEntry evidenceEntry : variantAnnotation.getTraitAssociation()) {
//                if (evidenceEntry.getSource() != null && StringUtils.isNotEmpty(evidenceEntry.getSource().getName())) {
//                    clinicalSource.add(evidenceEntry.getSource().getName().toLowerCase());
//                }
//                if (evidenceEntry.getVariantClassification() != null) {
//                    ClinicalSignificance clinicalSignificance = evidenceEntry.getVariantClassification().getClinicalSignificance();
//                    if (clinicalSignificance != null) {
//                        if (clinicalSignificance.equals(ClinicalSignificance.VUS)) {
//                            clinicalSignificance = ClinicalSignificance.uncertain_significance;
//                        }
//                        clinicalSignificances.add(clinicalSignificance.toString());
//                    }
//                }
//            }

            for (String combination : combinations) {
                if (combination.startsWith("cosmic")) {
                    source.add("cosmic");
                } else if (combination.startsWith("clinvar")) {
                    source.add("clinvar");
                }
            }
            schema.getClinicalIndexSchema().getSourceField().write(new ArrayList<>(source), clinicalIndex);
            schema.getClinicalIndexSchema().getClinicalSignificanceField().write(combinations, clinicalIndex);
        }
        return new AnnotationIndexEntry(b, intergenic, ctIndex.toInt(), btIndex.toInt(), ctBtCombination, popFreq, clinical, clinicalIndex);
    }
    protected void addPopFreqIndex(BitBuffer bitBuffer, PopulationFrequency populationFrequency) {
        IndexField<Double> field = schema.getPopFreqIndex()
                .getField(populationFrequency.getStudy(), populationFrequency.getPopulation());
        if (field != null) {
            field.write(populationFrequency.getAltAlleleFreq().doubleValue(), bitBuffer);
        }
    }

}
