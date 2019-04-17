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
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
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
    public static final Set<String> BIOTYPE_SET = new HashSet<>();
    public static final Set<String> POP_FREQ_ANY_001_SET = new HashSet<>();
    public static final Set<String> POP_FREQ_ANY_001_FILTERS = new HashSet<>();

    public static final byte BIOTYPE_MASK             = (byte) (1 << 0);
    public static final byte POP_FREQ_ANY_001_MASK    = (byte) (1 << 1);
    public static final byte LOFE_PROTEIN_CODING_MASK = (byte) (1 << 2);
    public static final byte LOF_EXTENDED_MASK        = (byte) (1 << 3);
    public static final byte LOF_MASK                 = (byte) (1 << 4);
    public static final byte CLINICAL_MASK            = (byte) (1 << 5);
    public static final byte LOF_EXTENDED_BASIC_MASK  = (byte) (1 << 6);
    public static final byte UNUSED_7_MASK            = (byte) (1 << 7);

    public static final byte[] COLUMN_FMAILY = Bytes.toBytes("0");
    public static final byte[] VALUE_COLUMN = Bytes.toBytes("v");
    public static final int VALUE_LENGTH = 1;
    public static final String TRANSCRIPT_FLAG_BASIC = "basic";

    static {

        BIOTYPE_SET.add(VariantAnnotationUtils.IG_C_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.IG_D_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.IG_J_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.IG_V_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.PROTEIN_CODING);
        BIOTYPE_SET.add(VariantAnnotationUtils.NONSENSE_MEDIATED_DECAY);
        BIOTYPE_SET.add(VariantAnnotationUtils.NON_STOP_DECAY);
        BIOTYPE_SET.add(VariantAnnotationUtils.TR_C_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.TR_D_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.TR_J_GENE);
        BIOTYPE_SET.add(VariantAnnotationUtils.TR_V_GENE);

        POP_FREQ_ANY_001_SET.add("1kG_phase3:ALL");
        POP_FREQ_ANY_001_SET.add("GNOMAD_GENOMES:ALL");

        for (String s : POP_FREQ_ANY_001_SET) {
            POP_FREQ_ANY_001_FILTERS.add(s + "<" + POP_FREQ_THRESHOLD_001);
        }

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

        if (variantAnnotation.getConsequenceTypes() != null) {
            for (ConsequenceType ct : variantAnnotation.getConsequenceTypes()) {
                if (BIOTYPE_SET.contains(ct.getBiotype())) {
                    b |= BIOTYPE_MASK;
                }
                boolean proteinCoding = VariantAnnotationUtils.PROTEIN_CODING.equals(ct.getBiotype());
                boolean basic = ct.getTranscriptAnnotationFlags() != null && ct.getTranscriptAnnotationFlags()
                        .contains(TRANSCRIPT_FLAG_BASIC);
                for (SequenceOntologyTerm sequenceOntologyTerm : ct.getSequenceOntologyTerms()) {
                    String soName = sequenceOntologyTerm.getName();
                    if (VariantQueryUtils.LOF_SET.contains(soName)) {
                        b |= LOF_MASK;
                        b |= LOF_EXTENDED_MASK;
                        if (basic) {
                            b |= LOF_EXTENDED_BASIC_MASK;
                        }
                        if (proteinCoding) {
                            b |= LOFE_PROTEIN_CODING_MASK;
                        }
                    } else if (VariantAnnotationUtils.MISSENSE_VARIANT.equals(soName)) {
                        b |= LOF_EXTENDED_MASK;
                        if (basic) {
                            b |= LOF_EXTENDED_BASIC_MASK;
                        }
                        if (proteinCoding) {
                            b |= LOFE_PROTEIN_CODING_MASK;
                        }
                    }
                }
            }
        }

        // By default, population frequency is 0.
        double minFreq = 0;
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
            }
            minFreq = Math.min(gnomadFreq, kgenomesFreq);
        }
        if (minFreq < POP_FREQ_THRESHOLD_001) {
            b |= POP_FREQ_ANY_001_MASK;
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
