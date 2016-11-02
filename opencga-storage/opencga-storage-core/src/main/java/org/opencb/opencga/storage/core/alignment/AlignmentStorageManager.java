package org.opencb.opencga.storage.core.alignment;

import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by pfurio on 31/10/16.
 */
public abstract class AlignmentStorageManager extends StorageManager<AlignmentDBAdaptor> {

    protected AlignmentStorageETL storageETL;
    protected AlignmentDBAdaptor dbAdaptor;

    public enum Options {
        MEAN_COVERAGE_SIZE_LIST ("mean_coverage_size_list", Arrays.asList("200", "10000")),
        PLAIN ("plain", false),
        TRANSFORM_REGION_SIZE ("transform.region_size", 200000),
        TRANSFORM_COVERAGE_CHUNK_SIZE ("transform.coverage_chunk_size", 1000),
        WRITE_COVERAGE ("transform.write_coverage", true),
        STUDY ("study", true),
        FILE_ID ("fileId", ""),
        FILE_ALIAS ("fileAlias", ""),
        WRITE_ALIGNMENTS ("writeAlignments", false),
        INCLUDE_COVERAGE ("includeCoverage", true),
        CREATE_BAM_INDEX ("createBai", true),
        ADJUST_QUALITY("adjustQuality", false),
        ENCRYPT ("encrypt", false),
        COPY_FILE ("copy", false),
        DB_NAME ("database.name", "opencga"),
        @Deprecated
        TOOLS_SAMTOOLS("tools.samtools", null);

        private final String key;
        private final Object value;

        Options(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        @SuppressWarnings("unchecked")
        public <T> T defaultValue() {
            return (T) value;
        }
    }

    public AlignmentStorageManager() {
    }

    @Deprecated
    public AlignmentStorageManager(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        logger = LoggerFactory.getLogger(AlignmentStorageManager.class);
    }

    public AlignmentStorageManager(AlignmentStorageETL storageETL, AlignmentDBAdaptor dbAdaptor, StorageConfiguration configuration) {
        super(configuration);
        this.storageETL = storageETL;
        this.dbAdaptor = dbAdaptor;
        logger = LoggerFactory.getLogger(AlignmentStorageManager.class);
    }

    public AlignmentGlobalStats getStats(String fileId) throws Exception {
        return getDBAdaptor().stats(fileId);
    }
}
