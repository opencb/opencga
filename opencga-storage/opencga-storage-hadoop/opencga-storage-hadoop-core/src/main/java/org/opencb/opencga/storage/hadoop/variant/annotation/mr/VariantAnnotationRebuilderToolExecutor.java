package org.opencb.opencga.storage.hadoop.variant.annotation.mr;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;

@ToolExecutor(id = "hbase-mapreduce", tool = "variant-annotation-rebuilder",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.STORAGE)
public class VariantAnnotationRebuilderToolExecutor extends OpenCgaToolExecutor implements HadoopVariantStorageToolExecutor {

    @Override
    protected void run() throws Exception {
        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        try {
            ObjectMap params = new ObjectMap(engine.getOptions());

            engine.getMRExecutor().run(VariantAnnotationRebuilderDriver.class, VariantAnnotationRebuilderDriver.buildArgs(
                    engine.getVariantTableName(),
                    params
            ), "Execute Variant Annotation Rebuilder Tool");
        } catch (VariantQueryException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        }

    }

}
