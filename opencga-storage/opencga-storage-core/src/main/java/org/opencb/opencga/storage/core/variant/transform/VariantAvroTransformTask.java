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
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.io.avro.AvroEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 01/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroTransformTask extends VariantTransformTask<ByteBuffer> {

    protected final AvroEncoder<VariantAvro> encoder;

    public VariantAvroTransformTask(VariantFactory factory, String studyId, VariantFileMetadata fileMetadata,
                                    VariantSetStatsCalculator variantStatsTask, boolean includeSrc, boolean generateReferenceBlocks) {
        super(factory, studyId, fileMetadata, variantStatsTask, includeSrc, generateReferenceBlocks);
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }

    public VariantAvroTransformTask(VCFHeader header, VCFHeaderVersion version, String studyId, VariantFileMetadata fileMetadata,
                                    VariantSetStatsCalculator variantStatsTask, boolean includeSrc,
                                    boolean generateReferenceBlocks) {
        super(header, version, studyId, fileMetadata, variantStatsTask, includeSrc, generateReferenceBlocks);
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }


    @Override
    protected List<ByteBuffer> encodeVariants(List<Variant> variants) {
        List<VariantAvro> avros = new ArrayList<>(variants.size());
        variants.forEach(variant -> avros.add(variant.getImpl()));
        List<ByteBuffer> encoded;
        try {
            encoded = encoder.encode(avros);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return encoded;
    }
}
