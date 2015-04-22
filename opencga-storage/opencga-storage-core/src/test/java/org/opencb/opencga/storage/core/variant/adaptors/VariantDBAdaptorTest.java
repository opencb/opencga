package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;

/**
 * Created by hpccoll1 on 13/03/15.
 */
@Ignore
public abstract class VariantDBAdaptorTest extends VariantStorageManagerTest {

    private static ETLResult etlResult;
    private VariantDBAdaptor dbAdaptor;

    @BeforeClass
    public static void beforeClass() {
        etlResult = null;
    }

    @Before
    public void before() throws Exception {
        if (etlResult == null) {
            StudyConfiguration studyConfiguration = newStudyConfiguration();
            clearDB();
            etlResult = runDefaultETL(getVariantStorageManager(), studyConfiguration);
        }
        dbAdaptor = getVariantStorageManager().getDBAdaptor(null, null);
    }

    @After
    public void after() {
        dbAdaptor.close();
    }





}
