/*
 * Copyright 2015-2016 OpenCB
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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.opencb.opencga.storage.alignment.hbase;

import org.opencb.biodata.models.alignment.AlignmentHeader;

/**
 *
 * @author jacobo
 */
public class AlignmentHBaseHeader  {
    private final boolean snappyCompress;
    private final int bucketSize;
    private final AlignmentHeader header;

    public AlignmentHBaseHeader(){
        this(null, 0, false);
    }
    public AlignmentHBaseHeader(AlignmentHeader header, int bucketSize, boolean snappy) {
//        super(header.getAttributes(), header.getSequenceDiccionary(), header.getReadGroups(), header.getProgramRecords(), header.getComments());
//        super.setSampleName(header.getSampleName());
//        super.setStudyName(header.getStudyName());
//        super.setOrigin(header.getOrigin());
        this.header = header;
        this.bucketSize = bucketSize;
        this.snappyCompress = snappy;
    }

    
    
    public int getBucketSize() {
        return bucketSize;
    }

    public boolean isSnappyCompress() {
        return snappyCompress;
    }


    public AlignmentHeader getHeader() {
        return header;
    }

    
    
}
