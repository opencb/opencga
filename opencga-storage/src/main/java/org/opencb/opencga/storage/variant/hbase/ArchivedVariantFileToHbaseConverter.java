package org.opencb.opencga.storage.variant.hbase;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.protobuf.VariantProtos;
import org.opencb.biodata.models.variant.protobuf.VariantStatsProtos;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.variant.StudyDBAdaptor;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class ArchivedVariantFileToHbaseConverter implements ComplexTypeConverter<ArchivedVariantFile, Put> {

    private boolean includeSamples;

    private List<String> samples;

    private VariantStatsToHbaseConverter statsConverter;
    private StudyDBAdaptor studyDbAdaptor;

    /**
     * Not-going-to-be-used row key, just necessary to satisfy HBase API.
     */
    private static byte[] rowkey = Bytes.toBytes("ArchivedVariantFileToHbaseConverter");
    
    
    /**
     * Create a converter between ArchivedVariantFile and HBase entities when
     * there is no need to provide a list of samples nor statistics.
     */
    public ArchivedVariantFileToHbaseConverter() {
        this(null, null);
    }

    /**
     * Create a converter from ArchivedVariantFile to HBase entities. A list of
     * samples and a statistics converter may be provided in case those should
     * be processed during the conversion.
     *
     * @param samples The list of samples, if any
     * @param statsConverter The object used to convert the file statistics
     */
    public ArchivedVariantFileToHbaseConverter(List<String> samples, VariantStatsToHbaseConverter statsConverter) {
        this.samples = samples;
        this.includeSamples = samples != null;
        this.statsConverter = statsConverter;
    }

    @Override
    public ArchivedVariantFile convertToDataModelType(Put object) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Put convertToStorageType(ArchivedVariantFile object) {
        Put put = new Put(rowkey);
        String prefix = object.getStudyId() + "_" + object.getFileId() + "_";

        // Attributes
        VariantProtos.VariantFileAttributes attrsProto = buildAttributesProto(object);
        put.add(VariantToHBaseConverter.COLUMN_FAMILY, Bytes.toBytes(prefix + "attrs"), attrsProto.toByteArray());
        put.add(VariantToHBaseConverter.COLUMN_FAMILY, Bytes.toBytes(prefix + "format"), Bytes.toBytes(object.getFormat()));

        // Samples
        if (samples != null && !samples.isEmpty()) {
            for (String sampleName : object.getSampleNames()) {
                VariantProtos.VariantSample sampleProto = buildSampleProto(object, sampleName);
                put.add(VariantToHBaseConverter.COLUMN_FAMILY, Bytes.toBytes(prefix + sampleName), sampleProto.toByteArray());
            }
        }
        
        // Statistics
        if (statsConverter != null) {
            VariantStatsProtos.VariantStats statsProto = statsConverter.convertToStorageType(object.getStats());
            put.add(VariantToHBaseConverter.COLUMN_FAMILY, Bytes.toBytes(prefix + "stats"), statsProto.toByteArray());
        }
        
        return put;
    }

    private VariantProtos.VariantFileAttributes buildAttributesProto(ArchivedVariantFile file) {
        VariantProtos.VariantFileAttributes.Builder builder = VariantProtos.VariantFileAttributes.newBuilder();

        for (Map.Entry<String, String> attr : file.getAttributes().entrySet()) {
            VariantProtos.VariantFileAttributes.KeyValue.Builder kvBuilder = VariantProtos.VariantFileAttributes.KeyValue.newBuilder();
            kvBuilder.setKey(attr.getKey());
            kvBuilder.setValue(attr.getValue());
            builder.addAttrs(kvBuilder.build());
        }

        return builder.build();
    }

    private VariantProtos.VariantSample buildSampleProto(ArchivedVariantFile file, String sampleName) {
        String joinedSampleFields = VcfUtils.getJoinedSampleFields(file, sampleName);
        VariantProtos.VariantSample.Builder builder = VariantProtos.VariantSample.newBuilder();
        builder.setSample(joinedSampleFields);
        return builder.build();
    }
    
}
