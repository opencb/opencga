/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.load;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntrySingleFileToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 30/05/17.
 *
 * This writer is thread-safe.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopDBWriter extends AbstractHBaseDataWriter<Variant, Put> {

    private final StudyEntryToHBaseConverter converter;
    private final AtomicInteger skippedRefBlock = new AtomicInteger();
    private final AtomicInteger skippedRefVariants = new AtomicInteger();
    private final AtomicInteger loadedVariants = new AtomicInteger();
    private final Logger logger = LoggerFactory.getLogger(VariantHadoopDBWriter.class);

    public VariantHadoopDBWriter(String tableName, int studyId, int fileId, VariantStorageMetadataManager metadataManager,
                                 HBaseManager hBaseManager, boolean includeReferenceVariantsData, boolean excludeGenotypes) {
        super(hBaseManager, tableName);
        int release = metadataManager.getProjectMetadata().getRelease();
        converter = new StudyEntrySingleFileToHBaseConverter(GenomeHelper.COLUMN_FAMILY_BYTES, studyId, fileId, metadataManager, true,
                release, includeReferenceVariantsData, excludeGenotypes);
    }

    @Override
    protected List<Put> convert(List<Variant> list) {
        List<Put> puts = new ArrayList<>(list.size());
        for (Variant variant : list) {
            if (HadoopVariantStorageEngine.TARGET_VARIANT_TYPE_SET.contains(variant.getType())) {
                Put put = converter.convert(variant);
                if (put != null) {
                    HadoopVariantSearchIndexUtils.addUnknownSyncStatus(put);
                    puts.add(put);
                    loadedVariants.getAndIncrement();
                } else {
                    skippedRefVariants.getAndIncrement();
                }
            } else { //Discard ref_block and symbolic variants.
                skippedRefBlock.getAndIncrement();
            }
        }
        return puts;
    }

    public int getSkippedRefBlock() {
        return skippedRefBlock.get();
    }

    public int getSkippedRefVariants() {
        return skippedRefVariants.get();
    }

    public int getLoadedVariants() {
        return loadedVariants.get();
    }

    public static List<Variant> filterVariantsNotFromThisSlice(long sliceStart, List<Variant> inputVariants) {
        List<Variant> variants = new ArrayList<>(inputVariants);
        variants.removeIf(variant -> variant.getStart() < sliceStart || variant.getEnd() < sliceStart);
        return variants;
    }
}
