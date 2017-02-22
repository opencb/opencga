package org.opencb.opencga.storage.core.alignment.iterators;

import ga4gh.Reads;
import org.opencb.biodata.tools.alignment.iterators.BamIterator;

/**
 * Created by pfurio on 26/10/16.
 */
public class ProtoAlignmentIterator extends AlignmentIterator<Reads.ReadAlignment> {

    private BamIterator<Reads.ReadAlignment> protoIterator;

    public ProtoAlignmentIterator(BamIterator<Reads.ReadAlignment> protoIterator) {
        this.protoIterator = protoIterator;
    }

    @Override
    public void close() throws Exception {
        protoIterator.close();
    }

    @Override
    public boolean hasNext() {
        return protoIterator.hasNext();
    }

    @Override
    public Reads.ReadAlignment next() {
        return protoIterator.next();
    }
}
