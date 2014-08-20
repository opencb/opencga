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
