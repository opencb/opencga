package org.opencb.opencga.storage.hadoop.variant.converters.study;

import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.util.*;

public class StudyEntrySingleFileToHBaseConverter extends StudyEntryToHBaseConverter {

    private final List<Integer> fileIdList;
    private final Map<Integer, byte[]> sampleToColumn;
    private final byte[] fileColumn;

    public StudyEntrySingleFileToHBaseConverter(byte[] columnFamily, int studyId, int fileId, VariantStorageMetadataManager metadataManager,
                                                boolean addSecondaryAlternates, Integer release, boolean includeReferenceVariantsData,
                                                boolean excludeGenotypes) {
        super(columnFamily, studyId, metadataManager, addSecondaryAlternates, release, excludeGenotypes, includeReferenceVariantsData);
        fileIdList = Collections.singletonList(fileId);

        sampleToColumn = new HashMap<>();

        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
        if (fileMetadata.getType() == FileMetadata.Type.PARTIAL) {
            int virtualFileId = fileMetadata.getAttributes().getInt(FileMetadata.VIRTUAL_PARENT, -1);
            if (virtualFileId < 0) {
                throw new IllegalArgumentException("Missing virtual parent id from file '" + fileMetadata.getName() + "'");
            }
            fileColumn = VariantPhoenixSchema.buildFileColumnKey(studyMetadata.getId(), virtualFileId);
        } else {
            fileColumn = VariantPhoenixSchema.buildFileColumnKey(studyMetadata.getId(), fileId);
        }
        metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
            int sampleId = sampleMetadata.getId();
            if (VariantStorageEngine.SplitData.MULTI == sampleMetadata.getSplitData()) {
                if (sampleMetadata.getFiles().indexOf(fileId) == 0) {
                    sampleToColumn.put(sampleId, VariantPhoenixSchema.buildSampleColumnKey(studyMetadata.getId(), sampleId));
                } else {
                    sampleToColumn.put(sampleId, VariantPhoenixSchema.buildSampleColumnKey(studyMetadata.getId(), sampleId, fileId));
                }
            } else {
                sampleToColumn.put(sampleId, VariantPhoenixSchema.buildSampleColumnKey(studyMetadata.getId(), sampleId));
            }
        });
    }

    @Override
    protected byte[] getFileColumnKey(int fileId) {
        return fileColumn;
    }

    protected byte[] getSampleColumn(Integer sampleId) {
        return sampleToColumn.get(sampleId);
    }

    @Override
    protected Collection<? extends Integer> getFilesFromSample(Integer sampleId) {
        return fileIdList;
    }
}
