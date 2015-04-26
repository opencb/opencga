/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.alignment;

import org.junit.Test;

import java.nio.file.Paths;


public class AlignmentMetaDataDBAdaptorTest {

    @Test
    public void generalTest(){
        AlignmentMetaDataDBAdaptor adaptor = new AlignmentMetaDataDBAdaptor("/home/jacobo/appl/files-index.properties");
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/home/jacobo/Documentos/bioinfo/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste3.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste4.bam")));
        System.out.println(adaptor.registerPath(Paths.get("/tmp/noexiste2.bam")));
        System.out.println(adaptor.getBamFromIndex("1"));
        System.out.println(adaptor.getBaiFromIndex("1"));
    }

}