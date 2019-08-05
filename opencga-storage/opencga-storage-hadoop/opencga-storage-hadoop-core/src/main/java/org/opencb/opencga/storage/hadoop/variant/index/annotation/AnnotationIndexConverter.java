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
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConfiguration;

import java.util.*;

import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory.generateVariantRowKey;

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
    public static final byte INTERGENIC_MASK =                   (byte) (1 << 7);  // ONLY INTERGENIC!!


    public static final short CT_MISSENSE_VARIANT_MASK =         (short) (1 << 0);
    public static final short CT_FRAMESHIFT_VARIANT_MASK =       (short) (1 << 1);
    public static final short CT_INFRAME_DELETION_MASK =         (short) (1 << 2);
    public static final short CT_INFRAME_INSERTION_MASK =        (short) (1 << 3);
    public static final short CT_START_LOST_MASK =               (short) (1 << 4);
    public static final short CT_STOP_GAINED_MASK =              (short) (1 << 5);
    public static final short CT_STOP_LOST_MASK =                (short) (1 << 6);
    public static final short CT_SPLICE_ACCEPTOR_VARIANT_MASK =  (short) (1 << 7);
    public static final short CT_SPLICE_DONOR_VARIANT_MASK =     (short) (1 << 8);
    public static final short CT_TRANSCRIPT_ABLATION_MASK =      (short) (1 << 9);
    public static final short CT_TRANSCRIPT_AMPLIFICATION_MASK = (short) (1 << 10);
    public static final short CT_INITIATOR_CODON_VARIANT_MASK =  (short) (1 << 11);
    public static final short CT_SPLICE_REGION_VARIANT_MASK =    (short) (1 << 12);
    public static final short CT_INCOMPLETE_TERMINAL_CODON_VARIANT_MASK = (short) (1 << 13);
    public static final short CT_UTR_MASK =                      (short) (1 << 14);
    public static final short CT_MIRNA_TFBS_MASK =               (short) (1 << 15);


    public static final byte BT_NONSENSE_MEDIATED_DECAY_MASK =   (byte) (1 << 0);
    public static final byte BT_LNCRNA_MASK =                    (byte) (1 << 1);
    public static final byte BT_MIRNA_MASK =                     (byte) (1 << 2);
    public static final byte BT_PROCESSED_TRANSCRIPT_MASK =      (byte) (1 << 3);
    public static final byte BT_SNRNA_MASK =                     (byte) (1 << 4);
    public static final byte BT_SNORNA_MASK =                    (byte) (1 << 5);
    public static final byte BT_NON_STOP_DECAY_MASK =            (byte) (1 << 6);
    public static final byte BT_PROTEIN_CODING_MASK =            (byte) (1 << 7);

    public static final byte[] COLUMN_FMAILY = Bytes.toBytes("0");
    public static final byte[] VALUE_COLUMN = Bytes.toBytes("v");
    public static final byte[] CT_VALUE_COLUMN = Bytes.toBytes("ct");
    public static final byte[] BT_VALUE_COLUMN = Bytes.toBytes("bt");
    public static final byte[] POP_FREQ_VALUE_COLUMN = Bytes.toBytes("pf");
    public static final int VALUE_LENGTH = 1;
    public static final String TRANSCRIPT_FLAG_BASIC = "basic";
    public static final int POP_FREQ_SIZE = 2;
    public static final String LNCRNA = "lncRNA";

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

    private final Map<String, Integer> populations;
    private final double[] popFreqRanges;

    @Deprecated
    public AnnotationIndexConverter() {
        this(SampleIndexConfiguration.defaultConfiguration());
    }

    public AnnotationIndexConverter(SampleIndexConfiguration configuration) {
        this.populations = new HashMap<>(configuration.getPopulationRanges().size());
        int i = 0;
        for (SampleIndexConfiguration.PopulationFrequencyRange population : configuration.getPopulationRanges()) {
            if (this.populations.put(population.getStudyAndPopulation(), i++) != null) {
                throw new IllegalArgumentException("Duplicated population '" + population.getStudyAndPopulation() + "' in " + populations);
            }
        }
        popFreqRanges = SampleIndexConfiguration.PopulationFrequencyRange.DEFAULT_THRESHOLDS;
    }

    public static Pair<Variant, AnnotationIndexEntry> getAnnotationIndexEntryPair(Result result) {
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow());
        byte summary = 0;
        short ct = 0;
        byte bt = 0;
        byte[] pf = null;

        for (Cell cell : result.rawCells()) {
            if (CellUtil.matchingQualifier(cell, VALUE_COLUMN)) {
                summary = cell.getValueArray()[cell.getValueOffset()];
            } else if (CellUtil.matchingQualifier(cell, CT_VALUE_COLUMN)) {
                ct = Bytes.toShort(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            } else if (CellUtil.matchingQualifier(cell, BT_VALUE_COLUMN)) {
                bt = cell.getValueArray()[cell.getValueOffset()];
            } else if (CellUtil.matchingQualifier(cell, POP_FREQ_VALUE_COLUMN)) {
                pf = CellUtil.cloneValue(cell);
            }
        }

        return Pair.of(variant,
                new AnnotationIndexEntry(summary, IndexUtils.testIndex(summary, INTERGENIC_MASK, INTERGENIC_MASK), ct, bt, pf));
    }

    public AnnotationIndexEntry convert(VariantAnnotation variantAnnotation) {
        if (variantAnnotation == null) {
            return AnnotationIndexEntry.empty(populations.size());
        }
        byte b = 0;
        short ctIndex = 0;
        byte btIndex = 0;
        byte[] popFreqIndex = new byte[populations.size()];

        boolean intergenic = false;

        if (variantAnnotation.getConsequenceTypes() != null) {
            for (ConsequenceType ct : variantAnnotation.getConsequenceTypes()) {
                if (BIOTYPE_SET.contains(ct.getBiotype())) {
                    b |= PROTEIN_CODING_MASK;
                }
                btIndex |= getMaskFromBiotype(ct.getBiotype());
                boolean proteinCoding = PROTEIN_CODING.equals(ct.getBiotype());
                for (SequenceOntologyTerm sequenceOntologyTerm : ct.getSequenceOntologyTerms()) {
                    String soName = sequenceOntologyTerm.getName();
                    if (!intergenic && INTERGENIC_VARIANT.equals(soName)) {
                        intergenic = true;
                    }
                    ctIndex |= getMaskFromSoName(soName);

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
                addPopFreqIndex(popFreqIndex, populationFrequency);
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
        }
        return new AnnotationIndexEntry(b, intergenic, ctIndex, btIndex, popFreqIndex);
    }

    protected void addPopFreqIndex(byte[] popFreqIndex, PopulationFrequency populationFrequency) {
        Integer idx = populations.get(populationFrequency.getStudy() + ":" + populationFrequency.getPopulation());
        if (idx != null) {
            byte popFreqInterval = IndexUtils.getRangeCode(populationFrequency.getAltAlleleFreq(), popFreqRanges);
//            int byteIdx = (idx * POP_FREQ_SIZE) / Byte.SIZE;
//            int bitIdx = (idx * POP_FREQ_SIZE) % Byte.SIZE;
//            popFreqIndex[byteIdx] |= popFreqInterval << bitIdx;
            popFreqIndex[idx] = popFreqInterval;
        }
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
        AnnotationIndexEntry value = convert(variantAnnotation);
        put.addColumn(COLUMN_FMAILY, VALUE_COLUMN, new byte[]{value.getSummaryIndex()});
        put.addColumn(COLUMN_FMAILY, CT_VALUE_COLUMN, Bytes.toBytes(value.getCtIndex()));
        put.addColumn(COLUMN_FMAILY, BT_VALUE_COLUMN, new byte[]{value.getBtIndex()});
        put.addColumn(COLUMN_FMAILY, POP_FREQ_VALUE_COLUMN, value.getPopFreqIndex());
        return put;
    }

    public static short getMaskFromSoName(String soName) {
        if (soName == null) {
            return 0;
        }

        switch (soName) {
            // Coding
            case MISSENSE_VARIANT:
                return CT_MISSENSE_VARIANT_MASK;
            case FRAMESHIFT_VARIANT:
                return CT_FRAMESHIFT_VARIANT_MASK;
            case INFRAME_DELETION:
                return CT_INFRAME_DELETION_MASK;
            case INFRAME_INSERTION:
                return CT_INFRAME_INSERTION_MASK;
            case START_LOST:
                return CT_START_LOST_MASK;
            case STOP_GAINED:
                return CT_STOP_GAINED_MASK;
            case STOP_LOST:
                return CT_STOP_LOST_MASK;
            case INITIATOR_CODON_VARIANT:
                return CT_INITIATOR_CODON_VARIANT_MASK;
            case SPLICE_REGION_VARIANT:
                return CT_SPLICE_REGION_VARIANT_MASK;
            case INCOMPLETE_TERMINAL_CODON_VARIANT:
                return CT_INCOMPLETE_TERMINAL_CODON_VARIANT_MASK;

            // Splice
            case SPLICE_ACCEPTOR_VARIANT:
                return CT_SPLICE_ACCEPTOR_VARIANT_MASK;
            case SPLICE_DONOR_VARIANT:
                return CT_SPLICE_DONOR_VARIANT_MASK;

            case TRANSCRIPT_ABLATION:
                return CT_TRANSCRIPT_ABLATION_MASK;
            case TRANSCRIPT_AMPLIFICATION:
                return CT_TRANSCRIPT_AMPLIFICATION_MASK;

            // Regulatory
//            case "regulatory_region_ablation":
//            case "regulatory_region_amplification":
//            case REGULATORY_REGION_VARIANT:
//                return CT_REGULATORY_MASK;

            case TF_BINDING_SITE_VARIANT:
//            case "TFBS_ablation":
//            case "TFBS_amplification":
            case MATURE_MIRNA_VARIANT:
                return CT_MIRNA_TFBS_MASK;

            // NonCoding
            case THREE_PRIME_UTR_VARIANT:
            case FIVE_PRIME_UTR_VARIANT:
                return CT_UTR_MASK;

//            // Intergenic
//            case UPSTREAM_GENE_VARIANT:
//            case TWOKB_UPSTREAM_VARIANT:
//            case "5KB_upstream_variant":
//
//            case DOWNSTREAM_GENE_VARIANT:
//            case TWOKB_DOWNSTREAM_VARIANT:
//            case "5KB_downstream_variant":
//                return CT_UPSTREAM_DOWNSTREAM_MASK;

            //case INTERGENIC_VARIANT:
            //case CODING_SEQUENCE_VARIANT:
            //case FEATURE_TRUNCATION:
            //case INFRAME_VARIANT:
            //case MISSENSE_VARIANT:
            //case NMD_TRANSCRIPT_VARIANT:
            //case STOP_RETAINED_VARIANT:
            //case TERMINATOR_CODON_VARIANT:
            //case "feature_elongation":
            //case "protein_altering_variant":
            //case SYNONYMOUS_VARIANT:
            //case INTRON_VARIANT:
            //case NON_CODING_TRANSCRIPT_EXON_VARIANT:
            //case NON_CODING_TRANSCRIPT_VARIANT:
            default:
                return 0;
        }
    }

    public static byte getMaskFromBiotype(String biotype) {
        if (biotype == null) {
            return 0;
        }

        switch (biotype) {
            case NONSENSE_MEDIATED_DECAY:
                return BT_NONSENSE_MEDIATED_DECAY_MASK;

            case LNCRNA:
            case LINCRNA:
            case ANTISENSE:
                return BT_LNCRNA_MASK;


            case MIRNA:
                return BT_MIRNA_MASK;
            case PROCESSED_TRANSCRIPT:
                return BT_PROCESSED_TRANSCRIPT_MASK;
            case SNRNA:
                return BT_SNRNA_MASK;
            case SNORNA:
                return BT_SNORNA_MASK;
            case NON_STOP_DECAY:
                return BT_NON_STOP_DECAY_MASK;
            case PROTEIN_CODING:
                return BT_PROTEIN_CODING_MASK;
            default:
                return 0;
        }
    }

}
