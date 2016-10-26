package org.opencb.opencga.storage.core.alignment.adaptors;

import org.junit.Test;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptorTest {

    @Test
    public void iterator() throws Exception {
        Path inputPath = Paths.get(getClass().getResource("/HG00096.chrom20.small.bam").toURI());
        DefaultAlignmentDBAdaptor defaultAlignmentDBAdaptor = new DefaultAlignmentDBAdaptor(inputPath);
        ProtoAlignmentIterator iterator = defaultAlignmentDBAdaptor.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
    }

}