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

package org.opencb.opencga.catalog.utils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.Individual;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

public class CatalogSampleAnnotationsLoaderTest extends GenericTest {

    private static final List<String> populations = Arrays.asList("ACB", "ASW", "BEB", "CDX", "CEU", "CHB", "CHS", "CLM", "ESN", "FIN",
            "GBR", "GIH", "GWD", "IBS", "ITU", "JPT", "KHV", "LWK", "MSL", "MXL", "PEL", "PJL", "PUR", "STU", "TSI", "YRI");
    private static CatalogSampleAnnotationsLoader loader;
    private static Pedigree pedigree;
    private static String sessionId;
    private static File pedFile;
    private static CatalogManager catalogManager;
    private static String userId;
    private static String studyId;

    @BeforeClass
    public static void beforeClass() throws IOException, CatalogException, URISyntaxException {
        Configuration configuration = Configuration.load(CatalogSampleAnnotationsLoaderTest.class.getClassLoader()
                .getClass().getResource("/configuration-test.yml").openStream());
        configuration.getAdmin().setAlgorithm("HS256");
        catalogManager = new CatalogManager(configuration);
        try {
            String token = catalogManager.getUserManager().loginAsAdmin("admin");
            catalogManager.deleteCatalogDB(token);
        } catch (Exception ignore) {}
        catalogManager.installCatalogDB("dummy", "admin", "opencga@admin.com", "");
        loader = new CatalogSampleAnnotationsLoader(catalogManager);

        String pedFileName = "20130606_g1k.ped";
        URL pedFileURL = CatalogSampleAnnotationsLoader.class.getClassLoader().getResource(pedFileName);
        pedigree = loader.readPedigree(pedFileURL.getPath());

        userId = "user1";
        catalogManager.getUserManager().create(userId, userId, "asdasd@asd.asd", userId, "", -1L, Account.Type.FULL, null);
        sessionId = catalogManager.getUserManager().login(userId, userId);
        Project project = catalogManager.getProjectManager().create("def", "default", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).getResults().get(0);
        Study study = catalogManager.getStudyManager().create(project.getFqn(), "def", null, "default", Study.Type.FAMILY, null, "", null, null, null, null, null, null, null, null, sessionId).getResults().get(0);
        studyId = study.getFqn();
        pedFile = catalogManager.getFileManager().upload(studyId, new FileInputStream(new java.io.File(pedFileURL.toURI())),
                new File().setPath("data/" + pedFileName), false, true, false, sessionId).first();
    }

    @AfterClass
    public static void afterClass() throws CatalogException {
//        catalogManager.logout(userId, sessionId);
    }

    @Test
    public void testLoadPedigree_GeneratedVariableSet() throws Exception {
        URL pedFile = this.getClass().getClassLoader().getResource("20130606_g1k.ped");

        Pedigree pedigree = loader.readPedigree(pedFile.getPath());
        VariableSet variableSet = loader.getVariableSetFromPedFile(pedigree);

        validate(pedigree, variableSet);
    }

    @Test
    public void testLoadPedigree_GivenVariableSet() throws Exception {
        HashSet<Variable> variables = new HashSet<>();
        variables.add(new Variable("id", "", Variable.VariableType.DOUBLE, null, true, false, Collections.<String>emptyList(), 0, null,
                "", null, null));
        variables.add(new Variable("name", "", Variable.VariableType.TEXT, null, true, false, Collections.<String>emptyList(), 0, null,
                "", null, null));
        variables.add(new Variable("fatherId", "", Variable.VariableType.DOUBLE, null, false, false, Collections.<String>emptyList(), 0,
                null, "", null, null));
        variables.add(new Variable("Population", "", Variable.VariableType.CATEGORICAL, null, true, false, populations, 0, null, "",
                null, null));
        variables.add(new Variable("NonExistingField", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, null, "",
                null, null));

        VariableSet variableSet = new VariableSet("", "", false, false, "", variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), 1, null);

        validate(pedigree, variableSet);
    }

    @Test
    public void testLoadPedigreeCatalog() throws Exception {
        DataResult<Sample> sampleDataResult = loader.loadSampleAnnotations(pedFile, null, sessionId);
        String variableSetId = sampleDataResult.getResults().get(0).getAnnotationSets().get(0).getVariableSetId();

        Query query = new Query(Constants.ANNOTATION, variableSetId + ":family=GB84");
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 0)
                .append("count", true);

        DataResult<Sample> allSamples = catalogManager.getSampleManager().search(studyId, query, options, sessionId);
        Assert.assertNotEquals(0, allSamples.getNumMatches());

        query = new Query(Constants.ANNOTATION, variableSetId + ":sex=2;" + variableSetId + ":Population=ITU");
        DataResult<Sample> femaleIta = catalogManager.getSampleManager().search(studyId, query, options, sessionId);
        Assert.assertNotEquals(0, femaleIta.getNumMatches());

        query = new Query(Constants.ANNOTATION, variableSetId + ":sex=1;" + variableSetId + ":Population=ITU");
        DataResult<Sample> maleIta = catalogManager.getSampleManager().search(studyId, query, options, sessionId);
        Assert.assertNotEquals(0, maleIta.getNumMatches());

        query = new Query(Constants.ANNOTATION, variableSetId + ":Population=ITU");
        DataResult<Sample> ita = catalogManager.getSampleManager().search(studyId, query, options, sessionId);
        Assert.assertNotEquals(0, ita.getNumMatches());

        Assert.assertEquals("Fail sample query", ita.getNumMatches(), maleIta.getNumMatches() + femaleIta.getNumMatches());
    }


    private void validate(Pedigree pedigree, VariableSet variableSet) throws CatalogException {
        for (Map.Entry<String, Individual> entry : pedigree.getIndividuals().entrySet()) {
            Map<String, Object> annotation = loader.getAnnotation(entry.getValue(), null, variableSet, pedigree.getFields());
            AnnotationUtils.checkAnnotationSet(variableSet, new AnnotationSet("", variableSet.getId(), annotation, "", 1,
                    null), null, true);
        }
    }
}