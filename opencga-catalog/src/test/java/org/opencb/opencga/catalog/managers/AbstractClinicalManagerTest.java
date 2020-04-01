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

package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

public class AbstractClinicalManagerTest extends GenericTest {

    public final static String PASSWORD = "password";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    public CatalogManager catalogManager;
    public String token;
    public String studyFqn;
    public Family family;
    public ClinicalAnalysis clinicalAnalysis;

    @Before
    public void setUp() throws IOException, CatalogException, URISyntaxException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException, URISyntaxException {

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);

        token = catalogManager.getUserManager().login("user", PASSWORD);

        catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens", null, "GRCh38",
                new QueryOptions(), token);

        Study study = catalogManager.getStudyManager().create("1000G", "phase1", null, "Phase 1", "Done", null, null,
                null, null, null, token).first();
        studyFqn = study.getFqn();

        family = catalogManager.getFamilyManager().create(studyFqn, getFamily(), QueryOptions.empty(), token).first();

        // Clinical analysis
        ClinicalAnalysis auxClinicalAnalysis = new ClinicalAnalysis()
                .setId("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setDueDate("20180510100000")
                .setDisorder(getDisorder())
                .setProband(getChild())
                .setFamily(getFamily());

        clinicalAnalysis = catalogManager.getClinicalAnalysisManager().create(studyFqn, auxClinicalAnalysis, QueryOptions.empty(), token)
                .first();

        URI familyVCF = getClass().getResource("/biofiles/family.vcf").toURI();

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new java.io.File(familyVCF)))) {
            catalogManager.getFileManager().upload(studyFqn, inputStream,
                    new File().setPath(Paths.get(familyVCF).getFileName().toString()), false, true, false, token);
        }
    }

    private Individual getMother() {
        return new Individual().setId("mother")
                .setDisorders(Collections.emptyList())
                .setSamples(Collections.singletonList(new Sample().setId("s2")));
    }

    private Individual getFather() {
        return new Individual().setId("father")
                .setDisorders(Collections.singletonList(getDisorder()))
                .setSamples(Collections.singletonList(new Sample().setId("s1")));
    }

    private Individual getChild() {
        return new Individual().setId("child")
                .setDisorders(Collections.singletonList(getDisorder()))
                .setFather(getFather())
                .setMother(getMother())
                .setSex(IndividualProperty.Sex.MALE)
                .setSamples(Collections.singletonList(new Sample().setId("s3")));
    }

    private Family getFamily () {
        return new Family("family", "family", null, null, Arrays.asList(getChild(), getFather(), getMother()), "", 3,
                Collections.emptyList(), Collections.emptyMap());
    }

    private Disorder getDisorder() {
        return new Disorder("disorder", "Disorder", "source", "description", null, null);
    }

}
