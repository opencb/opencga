/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;

/**
 * Created on 12/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantImportTest extends AbstractVariantOperationManagerTest {

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @Before
    public void setUp() throws Exception {

        indexFile(getSmallFile(), new QueryOptions(VariantStorageOptions.STATS_CALCULATE.key(), true), outputId);

        catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, Arrays.asList(
                new Variable("name", "", "", Variable.VariableType.STRING, null, true, false, null, null, 0, null, null, null, null),
                new Variable("age", "", "", Variable.VariableType.INTEGER, null, true, false, null, null, 0, null, null, null, null),
                new Variable("other", "", "", Variable.VariableType.STRING, "unknown", false, false, null, null, 0, null, null, null, null)),
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionId);

        catalogManager.getSampleManager().update(studyId, "NA19600", new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet("as1", "vs1",
                                new ObjectMap("name", "NA19600").append("age", 30)))), QueryOptions.empty(), sessionId);
        catalogManager.getSampleManager().update(studyId, "NA19660", new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet("as1", "vs1",
                                new ObjectMap("name", "NA19660").append("age", 35).append("other", "unknown")))),
                QueryOptions.empty(), sessionId);
        catalogManager.getSampleManager().update(studyId, "NA19660", new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet("as2", "vs1",
                                new ObjectMap("name", "NA19660").append("age", 35).append("other", "asdf")))),
                QueryOptions.empty(), sessionId);
    }

    @Test
    public void testExportImport() throws Exception {

        String export = Paths.get(opencga.createTmpOutdir(studyId, "_EXPORT_", sessionId)).resolve("export.json.gz").toString();

        variantManager.exportData(export, VariantOutputFormat.JSON_GZ, studyId, sessionId);

        DummyVariantStorageMetadataDBAdaptorFactory.clear();

        variantManager.importData(URI.create(export), studyId2, sessionId);

    }

    @Test
    public void testExportSomeSamplesImport() throws Exception {

        String export = Paths.get(opencga.createTmpOutdir(studyId, "_EXPORT_", sessionId)).resolve("export.avro").toString();

        List<Sample> samples = catalogManager.getSampleManager().search(studyId, new Query(), new QueryOptions(), sessionId).getResults();
        List<String> someSamples = samples.stream().limit(samples.size() / 2).map(Sample::getId).collect(Collectors.toList());
        Query query = new Query(VariantQueryParam.INCLUDE_STUDY.key(), studyId)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), someSamples);
        QueryOptions queryOptions = new QueryOptions();
        variantManager.exportData(export, VariantOutputFormat.AVRO, null, query, queryOptions, sessionId);

        DummyVariantStorageMetadataDBAdaptorFactory.clear();

        variantManager.importData(URI.create(export), studyId2, sessionId);

    }
}
