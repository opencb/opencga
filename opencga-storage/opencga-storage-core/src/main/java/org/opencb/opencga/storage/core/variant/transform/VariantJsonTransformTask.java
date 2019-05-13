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

package org.opencb.opencga.storage.core.variant.transform;

import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.opencb.biodata.formats.variant.VariantFactory;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantJsonTransformTask extends VariantTransformTask<String> {

    public VariantJsonTransformTask(VariantFactory factory, String studyId, VariantFileMetadata fileMetadata,
                                    VariantSetStatsCalculator variantStatsTask, boolean includesrc, boolean generateReferenceBlocks) {
        super(factory, studyId, fileMetadata, variantStatsTask, includesrc, generateReferenceBlocks);
    }

    public VariantJsonTransformTask(VCFHeader header, VCFHeaderVersion version, String studyId,
                                    VariantFileMetadata fileMetadata,
                                    VariantSetStatsCalculator variantStatsTask, boolean includeSrc, boolean generateReferenceBlocks) {
        super(header, version, studyId, fileMetadata, variantStatsTask, includeSrc, generateReferenceBlocks);
    }

    @Override
    protected List<String> encodeVariants(List<Variant> variants) {
        List<String> outputBatch = new ArrayList<>(variants.size() * 2);

        for (Variant variant : variants) {
            try {
                String e = variant.toJson();
                outputBatch.add(e);
            }  catch (Exception e) {
                logger.error("Error parsing variant: {}", variant);
                throw e;
            }
        }
        return outputBatch;
    }

}
