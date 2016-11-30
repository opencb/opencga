/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 16/12/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHBaseResultSetIterator extends VariantDBIterator {

    private final ResultSet resultSet;
    private final GenomeHelper genomeHelper;
    private final StudyConfigurationManager scm;
    private final HBaseToVariantConverter converter;
    private final Logger logger = LoggerFactory.getLogger(VariantHBaseResultSetIterator.class);

    private boolean hasNext = false;

    public VariantHBaseResultSetIterator(ResultSet resultSet, GenomeHelper genomeHelper, StudyConfigurationManager scm,
                                         QueryOptions options) throws SQLException {
        this(resultSet, genomeHelper, scm, options, Collections.emptyList());
    }

    public VariantHBaseResultSetIterator(ResultSet resultSet, GenomeHelper genomeHelper, StudyConfigurationManager scm,
                                         QueryOptions options, List<String> returnedSamples) throws SQLException {
        this.resultSet = resultSet;
        this.genomeHelper = genomeHelper;
        this.scm = scm;
        converter = new HBaseToVariantConverter(this.genomeHelper, this.scm)
                .setReturnedSamples(returnedSamples)
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(true)
                .setSimpleGenotypes(true);
        hasNext = fetch(resultSet::next);
    }

    public void skip(int skip) throws SQLException {
        if (skip > 0) {
            for (int count = 0; count < skip && hasNext; count++) {
                hasNext = fetch(resultSet::next);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms",
                getTimeFetching() / 1000000.0, getTimeConverting() / 1000000.0);
        resultSet.close();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public Variant next() {
        try {
            Variant variant = convert(() -> converter.convert(resultSet));
            hasNext = fetch(() -> resultSet.next());
            return variant;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
