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

package org.opencb.opencga.storage.hadoop.variant.transform;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfSliceToVariantListConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.transform.VariantStoragePipelineTransformTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.io.VariantReaderUtils.MALFORMED_FILE;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStoragePipelineTransformTest extends VariantStoragePipelineTransformTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();


    @Test
    public void protoTransformTest() throws Exception {

        URI platinumFile = getPlatinumFile(0);
        URI outputUri = newOutputUri();

        URI proto = transform(platinumFile, outputUri, "proto");
        URI avro = transform(platinumFile, outputUri, "avro");

        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(0, malformedFiles.length);

        LinkedHashSet<String> expectedVariants = variantStorageEngine.getVariantReaderUtils().getVariantReader(avro, null)
                .stream()
                .map(Variant::toString)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        LinkedHashSet<String> actualVariants = new LinkedHashSet<>();
        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(proto);
        VcfSliceToVariantListConverter converter = new VcfSliceToVariantListConverter(fileMetadata.toVariantStudyMetadata("S"));
        int numSlices = 0;
        int numVariants = 0;
        try (InputStream in = FileUtils.newInputStream(Paths.get(proto))) {
            VcfSliceProtos.VcfSlice vcfSlice;
            while ((vcfSlice = VcfSliceProtos.VcfSlice.parseDelimitedFrom(in)) != null) {
                List<Variant> convert = converter.convert(vcfSlice);
                for (Variant variant : convert) {
                    String str = variant.toString();
                    assertThat(expectedVariants, hasItems(str));
                    actualVariants.add(str);
                    numVariants++;
                }
                numSlices++;
            }
        }
        System.out.println("expectedVariants = " + expectedVariants.size());
        System.out.println("actualVariants = " + actualVariants.size());
        System.out.println("numSlices = " + numSlices);
        System.out.println("numVariants = " + numVariants);


        assertEquals(expectedVariants, actualVariants);


    }

    private URI transform(URI file, URI outputUri, String format) throws Exception {
        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        ObjectMap params = new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), format);
        StoragePipelineResult etlResult = runETL(variantStorageManager, file, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);
        return etlResult.getTransformResult();
    }

}
