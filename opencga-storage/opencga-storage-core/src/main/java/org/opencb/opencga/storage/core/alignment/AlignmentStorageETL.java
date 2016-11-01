package org.opencb.opencga.storage.core.alignment;

import org.opencb.opencga.storage.core.StorageETL;

/**
 * Created by pfurio on 31/10/16.
 */
public abstract class AlignmentStorageETL implements StorageETL {

    protected AlignmentDBAdaptor dbAdaptor;

    public AlignmentStorageETL(AlignmentDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
    }

}
