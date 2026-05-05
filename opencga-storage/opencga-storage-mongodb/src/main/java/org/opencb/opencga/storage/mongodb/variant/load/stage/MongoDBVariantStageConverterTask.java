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

package org.opencb.opencga.storage.mongodb.variant.load.stage;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.bson.Document;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStoragePipeline;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.STAGE_TO_VARIANT_CONVERTER;
import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.VARIANT_CONVERTER_DEFAULT;

/**
 * Created on 18/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageConverterTask implements Task<Variant, StageWriteOperations> {

    private final AtomicLong skippedVariants;
    private ComplexTypeConverter<Variant, Binary> variantConverter;

    public MongoDBVariantStageConverterTask() {
        this(VARIANT_CONVERTER_DEFAULT);
    }

    public MongoDBVariantStageConverterTask(ComplexTypeConverter<Variant, Binary> variantConverter) {
        skippedVariants = new AtomicLong(0);
        this.variantConverter = variantConverter;
    }

    @Override
    public void pre() {
        skippedVariants.set(0);
    }

    @Override
    public void post()  {
    }

    @Override
    public List<StageWriteOperations> apply(List<Variant> variants) throws RuntimeException {
        StageWriteOperations ids = convert(variants);
        return Collections.singletonList(ids);
    }

    public StageWriteOperations convert(List<Variant> variants) {
//        final long start = System.nanoTime();
        int localSkippedVariants = 0;

        ListMultimap<Document, Binary> ids = LinkedListMultimap.create();

        for (Variant variant : variants) {
            if (MongoDBVariantStoragePipeline.SKIPPED_VARIANTS.contains(variant.getType())) {
                localSkippedVariants++;
                continue;
            }
            Binary binary = variantConverter.convertToStorageType(variant);
            Document id = STAGE_TO_VARIANT_CONVERTER.convertToStorageType(variant);

            ids.put(id, binary);
        }

        skippedVariants.addAndGet(localSkippedVariants);
        return new StageWriteOperations(ids);
    }

    public long getSkippedVariants() {
        return skippedVariants.get();
    }
}
