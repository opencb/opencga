package org.opencb.opencga.storage.hadoop.variant.analysis.julie;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.JulieToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;

import java.util.stream.Collectors;

@ToolExecutor(id = "hbase-mapreduce", tool = "julie",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class JulieHBaseMapReduceToolExecutor extends JulieToolExecutor implements HadoopVariantStorageToolExecutor {

    @Override
    protected void run() throws Exception {
        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        try {
            VariantHadoopDBAdaptor dbAdaptor = engine.getDBAdaptor();

            ObjectMap params = new ObjectMap();

            if (getCohorts() != null && !getCohorts().isEmpty()) {
                String cohorts = getCohorts().entrySet()
                        .stream()
                        .map(e -> e.getKey() + ":" + e.getValue())
                        .collect(Collectors.joining(","));
                params.append(JulieToolDriver.COHORTS, cohorts);
            }
            params.append(VariantQueryParam.REGION.key(), getRegion());
            params.append(JulieToolDriver.OVERWRITE, getOverwrite());

            engine.getMRExecutor().run(JulieToolDriver.class, JulieToolDriver.buildArgs(
                    dbAdaptor.getVariantTable(),
                    params
            ), engine.getOptions(), "Execute Julie Tool");
        } catch (VariantQueryException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        }
    }

}
