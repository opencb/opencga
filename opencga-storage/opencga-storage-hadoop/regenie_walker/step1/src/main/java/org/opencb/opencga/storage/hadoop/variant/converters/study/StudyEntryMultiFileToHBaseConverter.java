package org.opencb.opencga.storage.hadoop.variant.converters.study;

import com.google.common.collect.LinkedListMultimap;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.util.Collection;

public class StudyEntryMultiFileToHBaseConverter extends StudyEntryToHBaseConverter {

    private final LinkedListMultimap<Integer, Integer> sampleToFileMap;

    public StudyEntryMultiFileToHBaseConverter(byte[] columnFamily, int studyId, VariantStorageMetadataManager metadataManager,
                                               boolean addSecondaryAlternates, Integer release, boolean includeReferenceVariantsData,
                                               boolean excludeGenotypes) {
        super(columnFamily, studyId, metadataManager, addSecondaryAlternates, release, excludeGenotypes, includeReferenceVariantsData);


        sampleToFileMap = LinkedListMultimap.create();

        metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
            sampleToFileMap.putAll(sampleMetadata.getId(), sampleMetadata.getFiles());
            if (VariantStorageEngine.SplitData.MULTI.equals(sampleMetadata.getSplitData())) {
                throw new IllegalArgumentException("Error loading multiple files!");
            }
        });
    }

    @Override
    protected byte[] getFileColumnKey(int fileId) {
        byte[] fileColumnKey = VariantPhoenixSchema
                .buildFileColumnKey(studyMetadata.getId(), fileId);
        return fileColumnKey;
    }

    @Override
    protected byte[] getSampleColumn(Integer sampleId) {
        return VariantPhoenixSchema.buildSampleColumnKey(studyMetadata.getId(), sampleId);
    }

    @Override
    protected Collection<? extends Integer> getFilesFromSample(Integer sampleId) {
        return sampleToFileMap.get(sampleId);
    }

}
