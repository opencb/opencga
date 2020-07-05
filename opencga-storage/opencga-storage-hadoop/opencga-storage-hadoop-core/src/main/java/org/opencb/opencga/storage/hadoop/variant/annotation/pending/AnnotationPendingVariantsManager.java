package org.opencb.opencga.storage.hadoop.variant.annotation.pending;

import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

public class AnnotationPendingVariantsManager extends PendingVariantsManager {

    public AnnotationPendingVariantsManager(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator) {
        super(hBaseManager, tableNameGenerator, new AnnotationPendingVariantsDescriptor());
    }

    public AnnotationPendingVariantsManager(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor, new AnnotationPendingVariantsDescriptor());
    }
}
