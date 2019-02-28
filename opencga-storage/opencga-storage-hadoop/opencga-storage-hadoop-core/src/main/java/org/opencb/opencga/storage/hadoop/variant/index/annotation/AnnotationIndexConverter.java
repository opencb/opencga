package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.PopulationFrequency;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;

/**
 * Created by jacobo on 04/01/19.
 */
public class AnnotationIndexConverter {

    public static final String GNOMAD_GENOMES = "GNOMAD_GENOMES";
    public static final String K_GENOMES = "1kG_phase3";

    public static final double POP_FREQ_THRESHOLD_001 = 0.001;
    public static final double POP_FREQ_THRESHOLD_01 = 0.01;
    // 0.01 con all_of las no ALL
//    public static final double POP_FREQ_THRESHOLD_005 = 0.005;
    public static final Set<String> LOF_SET = new HashSet<>(); // LOF does not include missense_variant
    public static final Set<String> LOF_SET_MISSENSE = new HashSet<>();
    public static final Set<String> PROTEIN_CODING_BIOTYPE_SET = new HashSet<>();
    public static final Set<String> POP_FREQ_ANY_001_SET = new HashSet<>();
    public static final Set<String> POP_FREQ_ALL_01_SET = new HashSet<>();

    public static final byte PROTEIN_CODING_MASK      = (byte) (1 << 0);
    public static final byte POP_FREQ_ANY_001_MASK    = (byte) (1 << 1);
    public static final byte POP_FREQ_ALL_01_MASK     = (byte) (1 << 2);
    public static final byte LOF_MISSENSE_MASK        = (byte) (1 << 3);
    public static final byte LOF_MASK                 = (byte) (1 << 4);
    public static final byte CLINICAL_MASK            = (byte) (1 << 5);
    public static final byte LOF_BASIC_MASK           = (byte) (1 << 6);
    public static final byte UNUSED_7_MASK            = (byte) (1 << 7);

    public static final byte[] COLUMN_FMAILY = Bytes.toBytes("0");
    public static final byte[] VALUE_COLUMN = Bytes.toBytes("v");
    public static final int VALUE_LENGTH = 1;

    static {
        LOF_SET.add(VariantAnnotationUtils.FRAMESHIFT_VARIANT);
        LOF_SET.add(VariantAnnotationUtils.INFRAME_DELETION);
        LOF_SET.add(VariantAnnotationUtils.INFRAME_INSERTION);
        LOF_SET.add(VariantAnnotationUtils.START_LOST);
        LOF_SET.add(VariantAnnotationUtils.STOP_GAINED);
        LOF_SET.add(VariantAnnotationUtils.STOP_LOST);
        LOF_SET.add(VariantAnnotationUtils.SPLICE_ACCEPTOR_VARIANT);
        LOF_SET.add(VariantAnnotationUtils.SPLICE_DONOR_VARIANT);
        LOF_SET.add(VariantAnnotationUtils.TRANSCRIPT_ABLATION);
        LOF_SET.add(VariantAnnotationUtils.TRANSCRIPT_AMPLIFICATION);
        LOF_SET.add(VariantAnnotationUtils.INITIATOR_CODON_VARIANT);
        LOF_SET.add(VariantAnnotationUtils.SPLICE_REGION_VARIANT);
        LOF_SET.add(VariantAnnotationUtils.INCOMPLETE_TERMINAL_CODON_VARIANT);

        LOF_SET_MISSENSE.addAll(LOF_SET);
        LOF_SET_MISSENSE.add(VariantAnnotationUtils.MISSENSE_VARIANT);

        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.IG_C_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.IG_D_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.IG_J_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.IG_V_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.PROTEIN_CODING);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.NONSENSE_MEDIATED_DECAY);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.NON_STOP_DECAY);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.TR_C_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.TR_D_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.TR_J_GENE);
        PROTEIN_CODING_BIOTYPE_SET.add(VariantAnnotationUtils.TR_V_GENE);

        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:AFR");
        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:AMR");
        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:EAS");
        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:FIN");
        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:NFE");
        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:ASJ");
        POP_FREQ_ALL_01_SET.add("GNOMAD_EXOMES:OTH");
        POP_FREQ_ALL_01_SET.add("1kG_phase3:AFR");
        POP_FREQ_ALL_01_SET.add("1kG_phase3:AMR");
        POP_FREQ_ALL_01_SET.add("1kG_phase3:EAS");
        POP_FREQ_ALL_01_SET.add("1kG_phase3:EUR");
        POP_FREQ_ALL_01_SET.add("1kG_phase3:SAS");

        POP_FREQ_ANY_001_SET.add("1kG_phase3:ALL");
        POP_FREQ_ANY_001_SET.add("GNOMAD_GENOMES:ALL");

    }

    public static Pair<Variant, Byte> getVariantBytePair(Result result) {
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
        Cell cell = result.getColumnLatestCell(COLUMN_FMAILY, VALUE_COLUMN);
        byte[] value = CellUtil.cloneValue(cell);
        return Pair.of(variant, value[0]);
    }

    public byte[] convert(List<VariantAnnotation> list) {
        byte[] bytes = new byte[list.size()];

        int i = 0;
        for (VariantAnnotation variantAnnotation : list) {
            bytes[i] = convert(variantAnnotation);
            i++;
        }

        return bytes;
    }

    public byte convert(VariantAnnotation variantAnnotation) {
        byte b = 0;

//        VariantType type = VariantBuilder.inferType(variantAnnotation.getReference(), variantAnnotation.getAlternate());
//        if (!type.equals(VariantType.SNV) && !type.equals(VariantType.SNP)) {
//            b |= UNUSED_6_MASK;
//        }

        if (variantAnnotation.getConsequenceTypes() != null) {
            for (ConsequenceType ct : variantAnnotation.getConsequenceTypes()) {
                if (PROTEIN_CODING_BIOTYPE_SET.contains(ct.getBiotype())) {
                    b |= PROTEIN_CODING_MASK;
                }
                for (SequenceOntologyTerm sequenceOntologyTerm : ct.getSequenceOntologyTerms()) {
                    String soName = sequenceOntologyTerm.getName();
                    if (LOF_SET.contains(soName)) {
                        b |= LOF_MASK;
                        b |= LOF_MISSENSE_MASK;
                        if (ct.getTranscriptAnnotationFlags() != null && ct.getTranscriptAnnotationFlags().contains("basic")) {
                            b |= LOF_BASIC_MASK;
                            break;
                        }
                    } else if (VariantAnnotationUtils.MISSENSE_VARIANT.equals(soName)) {
                        b |= LOF_MISSENSE_MASK;
                    }
                }
            }
        }

        // By default, population frequency is 0.
        double minFreq = 0;
        boolean popFreqAllLessThan01 = true;
        if (variantAnnotation.getPopulationFrequencies() != null) {
            double gnomadFreq = 0;
            double kgenomesFreq = 0;
            for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                if (populationFrequency.getPopulation().equals(DEFAULT_COHORT)) {
                    if (populationFrequency.getStudy().equals(GNOMAD_GENOMES)) {
                        gnomadFreq = populationFrequency.getAltAlleleFreq();
                    } else if (populationFrequency.getStudy().equals(K_GENOMES)) {
                        kgenomesFreq = populationFrequency.getAltAlleleFreq();
                    }
                }
                if (populationFrequency.getAltAlleleFreq() > POP_FREQ_THRESHOLD_01) {
                    if (POP_FREQ_ALL_01_SET.contains(populationFrequency.getStudy() + ':' + populationFrequency.getPopulation())) {
                        popFreqAllLessThan01 = false;
                    }
                }
            }
            minFreq = Math.min(gnomadFreq, kgenomesFreq);
        }
        if (minFreq < POP_FREQ_THRESHOLD_001) {
            b |= POP_FREQ_ANY_001_MASK;
        }
        if (popFreqAllLessThan01) {
            b |= POP_FREQ_ALL_01_MASK;
        }

        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            b |= CLINICAL_MASK;
        }
        return b;
    }

    public List<Put> convertToPut(List<VariantAnnotation> variantAnnotations) {
        List<Put> puts = new ArrayList<>(variantAnnotations.size());
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            puts.add(convertToPut(variantAnnotation));
        }
        return puts;
    }

    public Put convertToPut(VariantAnnotation variantAnnotation) {
        byte[] bytesRowKey = generateVariantRowKey(variantAnnotation);
        Put put = new Put(bytesRowKey);
        byte value = convert(variantAnnotation);
        put.addColumn(COLUMN_FMAILY, VALUE_COLUMN, new byte[]{value});
        return put;
    }
}
