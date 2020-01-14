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

package org.opencb.opencga.catalog.utils;

import org.junit.Test;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.models.file.File;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class BioformatDetectorTest {

    @Test
    public void detectVariant() {
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.vcf")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.vcf.variants.json")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.vcf.variants.json.gz")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.vcf.gz")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.vcf.gz.variants.json")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.vcf.gz.variants.json.gz")));

        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.variants.json")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.variants.json.gz")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.gz")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.gz.variants.json")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.gz.variants.json.gz")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.gz.variants.json.snappy")));
        assertEquals(File.Bioformat.VARIANT, FileUtils.detectBioformat(URI.create("file:///test.bcf.gz.variants.json.snz")));

        assertEquals(File.Bioformat.NONE, FileUtils.detectBioformat(URI.create("file:///test.vcf.tbi")));
        assertEquals(File.Bioformat.NONE, FileUtils.detectBioformat(URI.create("file:///test.vcf.txt")));
        assertEquals(File.Bioformat.NONE, FileUtils.detectBioformat(URI.create("file:///test.vcf.json")));
    }

    @Test
    public void detectAlignment() {
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.sam")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.cram")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.sam.gz")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam.alignments.json")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam.alignments.json.gz")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam.alignments.json.snz")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam.alignments.avro")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam.alignments.avro.gz")));
        assertEquals(File.Bioformat.ALIGNMENT, FileUtils.detectBioformat(URI.create("file:///test.bam.alignments.avro.snz")));
    }

    @Test
    public void detectPedigree() {
        assertEquals(File.Bioformat.PEDIGREE, FileUtils.detectBioformat(URI.create("file:///test.ped")));
        assertEquals(File.Bioformat.PEDIGREE, FileUtils.detectBioformat(URI.create("file:///test.ped.gz")));
    }

    @Test
    public void detectSequence() {
        assertEquals(File.Bioformat.SEQUENCE, FileUtils.detectBioformat(URI.create("file:///test.fastq")));
        assertEquals(File.Bioformat.SEQUENCE, FileUtils.detectBioformat(URI.create("file:///test.fastq.gz")));
    }

}