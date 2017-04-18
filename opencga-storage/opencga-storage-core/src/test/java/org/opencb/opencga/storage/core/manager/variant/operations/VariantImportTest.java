package org.opencb.opencga.storage.core.manager.variant.operations;

import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.storage.core.manager.variant.AbstractVariantStorageOperationTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;

import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;

/**
 * Created on 12/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantImportTest extends AbstractVariantStorageOperationTest {

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Test
    public void testExportImport() throws Exception {

        indexFile(getSmallFile(), new QueryOptions(VariantStorageEngine.Options.CALCULATE_STATS.key(), true), outputId);

        String export = Paths.get(opencga.createTmpOutdir(studyId, "_EXPORT_", sessionId)).resolve("export.avro.gz").toString();

        variantManager.exportData(export, VariantOutputFormat.AVRO_GZ, String.valueOf(studyId), sessionId);

        DummyStudyConfigurationAdaptor.clear();

        variantManager.importData(URI.create(export), String.valueOf(studyId2), sessionId);

    }

    @Test
    public void testExportSomeSamplesImport() throws Exception {

        indexFile(getSmallFile(), new QueryOptions(VariantStorageEngine.Options.CALCULATE_STATS.key(), true), outputId);

        String export = Paths.get(opencga.createTmpOutdir(studyId, "_EXPORT_", sessionId)).resolve("export.avro").toString();

        List<Sample> samples = catalogManager.getAllSamples(studyId, new Query(), new QueryOptions(), sessionId).getResult();
        List<String> someSamples = samples.stream().limit(samples.size() / 2).map(Sample::getName).collect(Collectors.toList());
        Query query = new Query(VariantQueryParam.RETURNED_STUDIES.key(), studyId)
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), someSamples);
        QueryOptions queryOptions = new QueryOptions();
        variantManager.exportData(export, VariantOutputFormat.AVRO, query, queryOptions, sessionId);

        DummyStudyConfigurationAdaptor.clear();

        variantManager.importData(URI.create(export), String.valueOf(studyId2), sessionId);

    }
}
