package org.opencb.opencga.storage.hadoop.variant.index.sample.family;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.net.URI;

/**
 * Hadoop / HBase implementation of {@link FamilyIndexTest}.
 */
@Category(MediumTests.class)
public class HadoopFamilyIndexTest extends FamilyIndexTest
        implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    protected void configureEngine() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        engine.getConfiguration().getCellbase().setUrl(ParamConstants.CELLBASE_URL);
        engine.getConfiguration().getCellbase().setVersion("v5.2");
        engine.getConfiguration().getCellbase().setDataRelease("3");
        engine.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), "grch38");
        engine.reloadCellbaseConfiguration();
    }

    @Override
    protected void fillGaps(Trio family, URI outputUri) throws Exception {
        variantStorageEngine.aggregateFamily(study,
                new VariantAggregateFamilyParams(family.toList(), false), new ObjectMap(), outputUri);
    }

    @Override
    protected void postLoad(URI outputUri) throws Exception {
        VariantHbaseTestUtils.printVariants(((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor(),
                newOutputUri(getTestName().getMethodName()));
    }
}
