package org.opencb.opencga.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Execution;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ExecutionManagerTest extends AbstractManagerTest {

    @Test
    public void submitExecutionWithDependencies() throws CatalogException {
        Execution execution1 = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM,
                new ObjectMap(), token).first();
        Execution execution2 = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM,
                new ObjectMap(), token).first();

        Execution execution3 = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM,
                new ObjectMap(), null, null, Arrays.asList(execution1.getId(), execution2.getId()), null, token).first();
        Execution execution4 = catalogManager.getExecutionManager().submit(studyFqn, "variant-index", Enums.Priority.MEDIUM,
                new ObjectMap(), null, null, Arrays.asList(execution1.getUuid(), execution2.getUuid()), null, token).first();

        assertEquals(2, execution3.getDependsOn().size());
        assertEquals(execution1.getUuid(), execution3.getDependsOn().get(0).getUuid());
        assertEquals(execution2.getUuid(), execution3.getDependsOn().get(1).getUuid());

        assertEquals(2, execution4.getDependsOn().size());
        assertEquals(execution1.getId(), execution4.getDependsOn().get(0).getId());
        assertEquals(execution2.getId(), execution4.getDependsOn().get(1).getId());
    }

}
