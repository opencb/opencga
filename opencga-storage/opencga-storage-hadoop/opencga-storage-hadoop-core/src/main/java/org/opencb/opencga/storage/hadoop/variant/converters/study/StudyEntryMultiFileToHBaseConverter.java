package org.opencb.opencga.storage.hadoop.variant.converters.study;

import com.google.common.collect.LinkedListMultimap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;

import java.util.Collection;

public class StudyEntryMultiFileToHBaseConverter extends StudyEntryToHBaseConverter {

    private final LinkedListMultimap<Integer, Integer> sampleToFileMap;

    public StudyEntryMultiFileToHBaseConverter(byte[] columnFamily, int studyId, VariantStorageMetadataManager metadataManager,
                                               boolean addSecondaryAlternates, Integer release, boolean includeReferenceVariantsData) {
        super(columnFamily, studyId, metadataManager, addSecondaryAlternates, release, includeReferenceVariantsData);


        sampleToFileMap = LinkedListMultimap.create();

        metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
            sampleToFileMap.putAll(sampleMetadata.getId(), sampleMetadata.getFiles());
            if (VariantStorageEngine.LoadSplitData.MULTI.equals(sampleMetadata.getSplitData())) {
                throw new IllegalArgumentException("Error loading multiple files!");
            }
        });
    }

    protected byte[] getSampleColumn(Integer sampleId) {
        return VariantPhoenixHelper.buildSampleColumnKey(studyMetadata.getId(), sampleId);
    }

    @Override
    protected Collection<? extends Integer> getFilesFromSample(Integer sampleId) {
        return sampleToFileMap.get(sampleId);
    }

}
