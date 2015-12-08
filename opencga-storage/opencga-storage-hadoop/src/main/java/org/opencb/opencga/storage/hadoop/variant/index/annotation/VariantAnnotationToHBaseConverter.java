package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.*;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created on 01/12/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationToHBaseConverter implements Converter<VariantAnnotation, Put> {


    public static final String ANNOTATION_COLUMN_PREFIX_STRING = "A_";
    public static final byte[] ANNOTATION_COLUMN_PREFIX = Bytes.toBytes(ANNOTATION_COLUMN_PREFIX_STRING);
    public static final byte[] FULL_ANNOTATION_COLUMN = Bytes.toBytes(ANNOTATION_COLUMN_PREFIX_STRING + "FULL_ANNOTATION");   // VARBINARY
    public static final byte[] GENES_COLUMN = Bytes.toBytes(ANNOTATION_COLUMN_PREFIX_STRING + "GENE");                        // VARCHAR[]
    public static final byte[] TRANSCRIPTS_COLUMN = Bytes.toBytes(ANNOTATION_COLUMN_PREFIX_STRING + "TRANSCRIPT");            // VARCHAR[]
    public static final byte[] BIOTYPE_COLUMN = Bytes.toBytes(ANNOTATION_COLUMN_PREFIX_STRING + "BIOTYPE");                   // VARCHAR[]
    public static final byte[] SO_COLUMN = Bytes.toBytes(ANNOTATION_COLUMN_PREFIX_STRING + "SO");                             // INTEGER[]


    public static final String POLYPHEN_FIELD = "polyphen";
    public static final String SIFT_FIELD = "sift";

    private final GenomeHelper genomeHelper;
    private boolean addFullAnnotation = true;

    public VariantAnnotationToHBaseConverter(GenomeHelper genomeHelper) {
        this.genomeHelper = genomeHelper;
    }

    @Override
    public Put convert(VariantAnnotation variantAnnotation) {
        byte[] bytesRowKey = genomeHelper.generateVariantRowKey(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                variantAnnotation.getReference(), variantAnnotation.getAlternate());

        Put put = new Put(bytesRowKey);

        if (addFullAnnotation) {
            put.addColumn(genomeHelper.getColumnFamily(), FULL_ANNOTATION_COLUMN, Bytes.toBytes(variantAnnotation.toString()));
        }

        Set<String> genes = new HashSet<>();
        Set<String> transcript = new HashSet<>();
        Set<Integer> so = new HashSet<>();
        Set<String> biotype = new HashSet<>();

        for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
            String value = consequenceType.getGeneName();
            addNotNull(genes, value);
            addNotNull(genes, consequenceType.getEnsemblGeneId());
            addNotNull(transcript, consequenceType.getEnsemblTranscriptId());
            addNotNull(biotype, consequenceType.getBiotype());
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                String accession = sequenceOntologyTerm.getAccession();
                addNotNull(so, Integer.parseInt(accession.substring(3)));
            }
        }

        addVarcharArray(put, GENES_COLUMN, genes);
        addVarcharArray(put, TRANSCRIPTS_COLUMN, transcript);
        addVarcharArray(put, BIOTYPE_COLUMN, biotype);
        addIntegerArray(put, SO_COLUMN, so);

        return put;
    }

    public <T> void addNotNull(Collection<T> collection, T value) {
        if (value != null) {
            collection.add(value);
        }
    }

    public void addVarcharArray(Put put, byte[] column, Collection<String> collection) {
        addArray(put,column, collection, PVarcharArray.INSTANCE);
    }

    public void addIntegerArray(Put put, byte[] column, Collection<Integer> collection) {
        addArray(put,column, collection, PIntegerArray.INSTANCE);
    }

    public void addArray(Put put, byte[] column, Collection collection, PArrayDataType arrayType) {
        if (collection.size() == 0) {
            return;
        }
        PDataType pDataType = PDataType.arrayBaseType(arrayType);
        Object[] elements = collection.toArray();
        PhoenixArray phoenixArray = new PhoenixArray(pDataType, elements);
        byte[] arrayBytes = arrayType.toBytes(phoenixArray);
        put.addColumn(genomeHelper.getColumnFamily(), column, arrayBytes);
    }

}
