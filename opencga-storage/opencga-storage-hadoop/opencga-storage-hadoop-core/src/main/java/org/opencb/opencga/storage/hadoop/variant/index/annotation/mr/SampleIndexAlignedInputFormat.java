package org.opencb.opencga.storage.hadoop.variant.index.annotation.mr;

import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;

/**
 * Created on 26/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexAlignedInputFormat extends VarinatAlignedInputFormat {

    @Override
    protected int getBatchSize() {
        return SampleIndexDBLoader.BATCH_SIZE;
    }
}