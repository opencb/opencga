/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.stats;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.db.VariantStatsDBWriter;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopDefaultVariantStatisticsManager extends DefaultVariantStatisticsManager {

    private static Logger logger = LoggerFactory.getLogger(HadoopDefaultVariantStatisticsManager.class);

    public HadoopDefaultVariantStatisticsManager(VariantDBAdaptor dbAdaptor, IOManagerProvider ioManagerProvider) {
        super(dbAdaptor, ioManagerProvider);
    }

    @Override
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts, Map<String, Integer> cohortIds,
                           StudyMetadata studyMetadata, QueryOptions options) throws IOException, StorageEngineException {
        if (options == null) {
            options = new QueryOptions();
        }
        options.putIfAbsent(QueryOptions.SKIP_COUNT, true);
        return super.createStats(variantDBAdaptor, output, cohorts, cohortIds, studyMetadata, options);
    }


    @Override
    protected VariantStatsDBWriter newVariantStatisticsDBWriter(VariantDBAdaptor dbAdaptor, StudyMetadata studyMetadata,
                                                                QueryOptions options) {
        if (!(dbAdaptor instanceof VariantHadoopDBAdaptor)) {
            throw new IllegalStateException("Expected " + VariantHadoopDBAdaptor.class + " dbAdaptor");
        }
        return new VariantStatsDBWriter(dbAdaptor, studyMetadata, options) {
            @Override
            public boolean pre() {
                super.pre();
                try {
                    ((VariantHadoopDBAdaptor) dbAdaptor).updateStatsColumns(studyMetadata);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }

        };
    }


}
