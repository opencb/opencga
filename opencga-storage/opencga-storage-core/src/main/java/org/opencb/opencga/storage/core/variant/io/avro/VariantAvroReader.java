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

package org.opencb.opencga.storage.core.variant.io.avro;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.io.avro.AvroDataReader;
import org.opencb.opencga.storage.core.variant.io.AbstractVariantReader;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 06/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroReader extends AbstractVariantReader {

    private final AvroDataReader<VariantAvro> avroDataReader;

    public VariantAvroReader(InputStream is, InputStream metaInputStream, VariantStudyMetadata metadata) {
        super(metaInputStream, metadata);
        avroDataReader = new AvroDataReader<>(is, VariantAvro.class);
    }

    public VariantAvroReader(InputStream is, File metadataFile, VariantStudyMetadata metadata) {
        super(metadataFile.toPath(), metadata);
        avroDataReader = new AvroDataReader<>(is, VariantAvro.class);
    }

    public VariantAvroReader(File variantsFile, File metadataFile, VariantStudyMetadata metadata) {
        super(metadataFile.toPath(), metadata);
        avroDataReader = new AvroDataReader<>(variantsFile, VariantAvro.class);
    }

    public VariantAvroReader(File variantsFile, Map<String, LinkedHashMap<String, Integer>> samplesPositions) {
        super(samplesPositions);
        avroDataReader = new AvroDataReader<>(variantsFile, VariantAvro.class);
    }

    @Override
    public boolean open() {
        return avroDataReader.open();
    }

    @Override
    public boolean close() {
        return avroDataReader.close();
    }

    @Override
    public boolean pre() {
        super.pre();
        return avroDataReader.pre();
    }

    @Override
    public boolean post() {
        return avroDataReader.post();
    }

    @Override
    public List<Variant> read(int batchSize) {
        List<Variant> batch = new ArrayList<>(batchSize);
        List<VariantAvro> read = avroDataReader.read(batchSize);
        for (VariantAvro variantAvro : read) {
            Variant variant = new Variant(variantAvro);
            batch.add(variant);
        }
        return addSamplesPosition(batch);
    }

}
