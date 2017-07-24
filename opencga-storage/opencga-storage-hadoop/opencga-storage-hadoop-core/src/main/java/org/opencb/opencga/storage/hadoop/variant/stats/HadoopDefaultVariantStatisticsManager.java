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
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.db.VariantStatsDBWriter;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.sql.SQLException;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopDefaultVariantStatisticsManager extends DefaultVariantStatisticsManager {
    public HadoopDefaultVariantStatisticsManager(VariantDBAdaptor dbAdaptor) {
        super(dbAdaptor);
    }

    @Override
    protected VariantStatsDBWriter newVariantStatisticsDBWriter(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration,
                                                                QueryOptions options) {
        if (!(dbAdaptor instanceof VariantHadoopDBAdaptor)) {
            throw new IllegalStateException("Expected " + VariantHadoopDBAdaptor.class + " dbAdaptor");
        }
        return new VariantStatsDBWriter(dbAdaptor, studyConfiguration, options) {
            @Override
            public boolean pre() {
                super.pre();
                try {
                    ((VariantHadoopDBAdaptor) dbAdaptor).updateStatsColumns(studyConfiguration);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }

        };
    }


}
