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

package org.opencb.opencga.storage.core.io;

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opencb.biodata.formats.variant.vcf4.FullVcfCodec;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.core.io.VcfVariantReader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 04/05/16.
 */
public class VcfVariantReaderTest {

    public VcfVariantReader reader;

    @Before
    public void setup(){
        reader = createReader(-1);
    }

    public static VcfVariantReader createReader(final int size) {
        VariantNormalizer normalizer = new VariantNormalizer();
        VariantGlobalStatsCalculator stats = Mockito.mock(VariantGlobalStatsCalculator.class);

        VariantContextToVariantConverter converter = new VariantContextToVariantConverter("1","1", Arrays.asList(
                "ABC"));

        VCFHeaderVersion version = VCFHeaderVersion.VCF4_2;
        VCFHeader header = buildHeader();
        DataReader<String> dr = new DataReader<String>() {
            private final String line = "1\t%s\t.\tT\tA\t.\tPASS\t.\tGT\t0/1";
//            private final List<String> one = Collections.singletonList(line);
            private int count = 0;

            @Override
            public List<String> read() {
                if (size != -1 && count > size)
                    return Collections.emptyList();
                ++ count;
                return Collections.singletonList(String.format(line, count));
            }

            @Override
            public List<String> read(int batchSize) {
                if (size != -1 && count > size)
                    return Collections.emptyList();
                List<String> arr = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    if (size != -1 && count > size)
                        break;
                    ++count;
                    arr.add(String.format(line,count));
                }
                return arr;
            }
        };
        return new VcfVariantReader(dr, header, version, converter, stats, normalizer);
    }

    @Test
    public void testProcessLine() throws Exception {
        List<Variant> vars = reader.read();
        assertEquals("Expect only one Variant inline",1,vars.size());
        Variant variant = vars.get(0);
        assertEquals("Reference wrong", "T", variant.getReference());
        assertEquals("Alt wrong", "A", variant.getAlternate());

        vars = reader.read(22);
        assertEquals("Expect only one Variant inline",22,vars.size());
    }

    public void testBatch(){
        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            List<Variant> lst = reader.read(1);
            lst.size();
        }
        System.out.println("Run for " + (System.currentTimeMillis() - curr) + " and measured " + reader.timesOverall.get());
    }

    public void testRuntime() {
        String line = "1\t1\t.\tT\tA\t.\tPASS\t.\tGT\t0/1";
        for (int i = 0; i < 5; i++) {
            long curr = System.currentTimeMillis();
            for (int j = 0; j < 100000; j++) {
                List<Variant> vars = reader.processLine(line);
            }
            System.out.println("Run for " + (System.currentTimeMillis() - curr));
        }
    }

    public static VCFHeader buildHeader(){
        FullVcfCodec codec = new FullVcfCodec();
        byte[] buf = ("##fileformat=VCFv4.1\n"
                + "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n"
                + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tABC\n").getBytes();
        InputStream fileInputStream = new ByteArrayInputStream(buf);
        LineIterator lineIterator = codec.makeSourceFromStream(fileInputStream);
        VCFHeader header = (VCFHeader) codec.readActualHeader(lineIterator);
        return header;
    }
}