package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

/**
 * Created on 01/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationToHBaseConverter implements Converter<VariantAnnotation, Put> {


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
            put.addColumn(genomeHelper.getColumnFamily(), FULL_ANNOTATION.bytes(), Bytes.toBytes(variantAnnotation.toString()));
        }

        Set<String> genes = new HashSet<>();
        Set<String> transcript = new HashSet<>();
        Set<Integer> so = new HashSet<>();
        Set<String> biotype = new HashSet<>();
        Set<Double> polyphen = new HashSet<>();
        Set<Double> sift = new HashSet<>();

        for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
            addNotNull(genes, consequenceType.getGeneName());
            addNotNull(genes, consequenceType.getEnsemblGeneId());
            addNotNull(transcript, consequenceType.getEnsemblTranscriptId());
            addNotNull(biotype, consequenceType.getBiotype());
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                String accession = sequenceOntologyTerm.getAccession();
                addNotNull(so, Integer.parseInt(accession.substring(3)));
            }
            if (consequenceType.getProteinVariantAnnotation() != null) {
                if (consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                    for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                        if (score.getSource().equalsIgnoreCase("sift")) {
                            addNotNull(sift, score.getScore());
                        } else if (score.getSource().equalsIgnoreCase("polyphen")) {
                            addNotNull(polyphen, score.getScore());
                        }
                    }
                }
            }
        }

        addVarcharArray(put, GENES.bytes(), genes);
        addVarcharArray(put, TRANSCRIPTS.bytes(), transcript);
        addVarcharArray(put, BIOTYPE.bytes(), biotype);
        addIntegerArray(put, SO.bytes(), so);
        addArray(put, POLYPHEN.bytes(), polyphen, (PArrayDataType) POLYPHEN.getPDataType());
        addArray(put, SIFT.bytes(), sift, (PArrayDataType) SIFT.getPDataType());

        if (variantAnnotation.getConservation() != null) {
            for (Score score : variantAnnotation.getConservation()) {
                put.addColumn(genomeHelper.getColumnFamily(), getConservationColumnName(score), PFloat.INSTANCE.toBytes(score.getScore()));
            }
        }

        if (variantAnnotation.getPopulationFrequencies() != null) {
            for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                put.addColumn(genomeHelper.getColumnFamily(), getPopulationFrequencyColumnName(populationFrequency),
                        PFloat.INSTANCE.toBytes(populationFrequency.getAltAlleleFreq()));
            }
        }

        return put;
    }

    private byte[] getPopulationFrequencyColumnName(PopulationFrequency populationFrequency) {
        return Bytes.toBytes(getPopulationFrequencyColumnName(populationFrequency.getStudy(),
                populationFrequency.getPopulation()));
    }

    private String getPopulationFrequencyColumnName(String study, String population) {
        return (study + ":" + population).toUpperCase();
    }

    private byte[] getConservationColumnName(Score score) {
        return Bytes.toBytes(score.getSource().toUpperCase());
    }

    public <T> void addNotNull(Collection<T> collection, T value) {
        if (value != null) {
            collection.add(value);
        }
    }

    public void addVarcharArray(Put put, byte[] column, Collection<String> collection) {
        addArray(put, column, collection, PVarcharArray.INSTANCE);
    }

    public void addIntegerArray(Put put, byte[] column, Collection<Integer> collection) {
        addArray(put, column, collection, PIntegerArray.INSTANCE);
    }

    public void addArray(Put put, byte[] column, Collection collection, PArrayDataType arrayType) {
        if (collection.size() == 0) {
            return;
        }
        byte[] arrayBytes = VariantPhoenixHelper.toBytes(collection, arrayType);
        put.addColumn(genomeHelper.getColumnFamily(), column, arrayBytes);
    }

}
