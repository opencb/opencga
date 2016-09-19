/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.Test;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.VariableSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 19/01/16.
 */
public class CatalogMongoStudyDBAdaptorTest extends CatalogMongoDBAdaptorTest {

    @Test
    public void updateDiskUsage() throws Exception {
        catalogDBAdaptor.getCatalogStudyDBAdaptor().updateDiskUsage(5, 100);
        assertEquals(2100, catalogStudyDBAdaptor.get(5, null).getResult().get(0).getDiskUsage());
        catalogDBAdaptor.getCatalogStudyDBAdaptor().updateDiskUsage(5, -200);
        assertEquals(1900, catalogStudyDBAdaptor.get(5, null).getResult().get(0).getDiskUsage());
    }

    /***
     * The test will check whether it is possible to create a new study using an alias that is already being used, but on a different
     * project.
     */
    @Test
    public void createStudySameAliasDifferentProject() throws CatalogException {
        QueryResult<Study> ph1 = catalogStudyDBAdaptor.insert(1, new Study("Phase 1", "ph1", Study.Type.CASE_CONTROL, "",
                new Status(), null), null);
        assertTrue("It is impossible creating an study with an existing alias on a different project.", ph1.getNumResults() == 1);
    }

    private QueryResult<VariableSet> createExampleVariableSet() throws CatalogDBException {
        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.NUMERIC, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap())
        ));
        VariableSet variableSet = new VariableSet(-1, "VARSET_1", false, "My description", variables, Collections.emptyMap());
        return catalogStudyDBAdaptor.createVariableSet(5L, variableSet);
    }

    @Test
    public void createVariableSetTest() throws CatalogDBException {
        QueryResult<VariableSet> queryResult = createExampleVariableSet();
        assertEquals("VARSET_1", queryResult.first().getName());
        assertTrue("The id of the variableSet is wrong.", queryResult.first().getId() > -1);
    }

    @Test
    public void testRemoveFieldFromVariableSet() throws CatalogDBException {
        QueryResult<VariableSet> variableSetQueryResult = createExampleVariableSet();
        catalogStudyDBAdaptor.removeFieldFromVariableSet(variableSetQueryResult.first().getId(), "NAME");
    }

    @Test
    public void testRenameFieldInVariableSet() throws CatalogDBException {
        QueryResult<VariableSet> variableSetQueryResult = createExampleVariableSet();
        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetQueryResult.first().getId(), "NAME", "NEW_NAME");
    }

    @Test
    public void testRenameFieldInVariableSetOldFieldNotExist() throws CatalogDBException {
        QueryResult<VariableSet> variableSetQueryResult = createExampleVariableSet();
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("NAM} does not exist.");
        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetQueryResult.first().getId(), "NAM", "NEW_NAME");
    }

    @Test
    public void testRenameFieldInVariableSetNewFieldExist() throws CatalogDBException {
        QueryResult<VariableSet> variableSetQueryResult = createExampleVariableSet();
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("The variable {id: AGE} already exists.");
        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetQueryResult.first().getId(), "NAME", "AGE");
    }

    @Test
    public void testRenameFieldInVariableSetVariableSetNotExist() throws CatalogDBException {
        createExampleVariableSet();
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("VariableSet id '-1' is not valid");
        catalogStudyDBAdaptor.renameFieldVariableSet(-1, "NAME", "NEW_NAME");
    }

    /**
     * Creates a new variable once and attempts to create the same one again.
     * @throws CatalogDBException
     */
    @Test
    public void addFieldToVariableSetTest1() throws CatalogDBException {
        createExampleVariableSet();
        Variable variable = new Variable("NAM", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "", null,
                Collections.emptyMap());
        QueryResult<VariableSet> queryResult = catalogStudyDBAdaptor.addFieldToVariableSet(18, variable);

        // Check that the new variable has been inserted in the variableSet
        assertTrue(queryResult.first().getVariables().stream().filter(variable1 -> variable.getName().equals(variable1.getName())).findAny()
                .isPresent());

        // We try to insert the same one again.
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exist");
        catalogStudyDBAdaptor.addFieldToVariableSet(18, variable);
    }

    /**
     * Tries to add a new variable to a non existent variableSet.
     * @throws CatalogDBException
     */
    @Test
    public void addFieldToVariableSetTest2() throws CatalogDBException {
        Variable variable = new Variable("NAM", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "", null,
                Collections.emptyMap());
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("does not exist");
        catalogStudyDBAdaptor.addFieldToVariableSet(18, variable);
    }


}
