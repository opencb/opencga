package org.opencb.opencga.core.tools.migration.v2_0_0;

import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ToolExecutor(id = "hbase-mapreduce", tool = "variant-storage-migration-2.0.0",
        framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class NoOpVariantStorage200MigrationToolExecutor extends VariantStorage200MigrationToolExecutor {

    private Logger logger = LoggerFactory.getLogger(NoOpVariantStorage200MigrationToolExecutor.class);

    @Override
    protected void run() throws Exception {
        logger.info("Nothing to do!");
    }
}
