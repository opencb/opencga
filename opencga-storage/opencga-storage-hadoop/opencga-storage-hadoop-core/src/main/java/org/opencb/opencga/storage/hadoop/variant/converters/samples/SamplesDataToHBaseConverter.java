package org.opencb.opencga.storage.hadoop.variant.converters.samples;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created on 25/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SamplesDataToHBaseConverter extends AbstractPhoenixConverter implements Converter<Variant, Put> {


    private final Set<String> defaultGenotypes = new HashSet<>();
    private final StudyConfiguration studyConfiguration;

    public SamplesDataToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration) {
        super(columnFamily);
        this.studyConfiguration = studyConfiguration;
        defaultGenotypes.add("0/0");
        defaultGenotypes.add("0|0");
    }

    @Override
    public Put convert(Variant variant) {
        byte[] rowKey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
        Put put = new Put(rowKey);
        return convert(variant, put, null);
    }

    public Put convert(Variant variant, Put put) {
        return convert(variant, put, null);
    }


    public Put convert(Variant variant, Put put, Set<Integer> sampleIds) {
        StudyEntry studyEntry = variant.getStudies().get(0);
        Integer gtIdx = studyEntry.getFormatPositions().get(VariantMerger.GT_KEY);
        int sampleIdx = 0;
        for (String sampleName : studyEntry.getOrderedSamplesName()) {
            Integer sampleId = studyConfiguration.getSampleIds().get(sampleName);
            if (sampleIds == null || sampleIds.contains(sampleId)) {
                byte[] column = VariantPhoenixHelper.buildSampleColumnKey(studyConfiguration.getStudyId(), sampleId);
                List<String> sampleData = studyEntry.getSamplesData().get(sampleIdx);
                if (!defaultGenotypes.contains(sampleData.get(gtIdx))) {
                    addVarcharArray(put, column, sampleData);
                }
            }
            sampleIdx++;
        }

        return put;
    }

}
