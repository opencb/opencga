package org.opencb.opencga.storage.core.alignment.iterators;

import htsjdk.samtools.SAMRecord;

/**
 * Created by pfurio on 04/11/16.
 */
public class SamRecordAlignmentIterator extends AlignmentIterator<SAMRecord> {

    private org.opencb.biodata.tools.alignment.iterators.AlignmentIterator<SAMRecord> samIterator;

    public SamRecordAlignmentIterator(org.opencb.biodata.tools.alignment.iterators.AlignmentIterator<SAMRecord> samIterator) {
        this.samIterator = samIterator;
    }

    @Override
    public void close() throws Exception {
        samIterator.close();
    }

    @Override
    public boolean hasNext() {
        return samIterator.hasNext();
    }

    @Override
    public SAMRecord next() {
        return samIterator.next();
    }
}
