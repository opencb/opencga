package org.opencb.opencga.storage.core.alignment.adaptors;

import org.junit.Test;
import org.opencb.opencga.storage.core.alignment.local.DefaultAlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptorTest {

    @Test
    public void iterator() throws Exception {
        String inputPath = getClass().getResource("/HG00096.chrom20.small.bam").toString();
        DefaultAlignmentDBAdaptor defaultAlignmentDBAdaptor = new DefaultAlignmentDBAdaptor();
        ProtoAlignmentIterator iterator = defaultAlignmentDBAdaptor.iterator(inputPath);
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
    }

}