package org.opencb.opencga.storage.core.alignment.iterators;

import htsjdk.samtools.SAMRecord;
import org.opencb.biodata.tools.alignment.iterators.BamIterator;

/**
 * Created by pfurio on 04/11/16.
 */
public class SamRecordAlignmentIterator extends AlignmentIterator<SAMRecord> {

    private BamIterator<SAMRecord> bamIterator;

    public SamRecordAlignmentIterator(BamIterator<SAMRecord> bamIterator) {
        this.bamIterator = bamIterator;
    }

    @Override
    public void close() throws Exception {
        bamIterator.close();
    }

    @Override
    public boolean hasNext() {
        return bamIterator.hasNext();
    }

    @Override
    public SAMRecord next() {
        return bamIterator.next();
    }
}
