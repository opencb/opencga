package org.opencb.opencga.storage.benchmark.variant;

import org.opencb.opencga.storage.benchmark.BenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.QueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineDirectSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineRestSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineSampler;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantBenchmarkRunner extends BenchmarkRunner {

    public enum ConnectionType {
        REST,
        DIRECT,
        GRPC,
    }

    public VariantBenchmarkRunner(StorageConfiguration storageConfiguration, Path jmeterHome, Path outdir) throws IOException {
        super(storageConfiguration, jmeterHome, outdir);
    }

    public void addThreadGroup(ConnectionType type, Path dataDir, List<Class<? extends QueryGenerator>> queryGenerators) {
        List<VariantStorageEngineSampler> samplers = new ArrayList<>(queryGenerators.size());
        for (Class<? extends QueryGenerator> clazz : queryGenerators) {
            VariantStorageEngineSampler variantStorageSampler = newVariantStorageEngineSampler(type);

            variantStorageSampler.setStorageEngine(storageEngine);
            variantStorageSampler.setDBName(dbName);
            variantStorageSampler.setQueryGenerator(clazz);
            variantStorageSampler.setDataDir(dataDir.toString());

            samplers.add(variantStorageSampler);
        }

        addThreadGroup(samplers);

    }

    public VariantStorageEngineSampler newVariantStorageEngineSampler(ConnectionType type) {
        switch (type) {
            case REST:
                return new VariantStorageEngineRestSampler("localhost", storageConfiguration.getServer().getRest());
            case DIRECT:
                return new VariantStorageEngineDirectSampler();
            case GRPC:
                throw new UnsupportedOperationException("Unsupported type " + ConnectionType.GRPC);
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }
}
