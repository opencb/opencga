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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalAnalysisUtilsTest {

    public static AbstractClinicalManagerTest getClinicalTest(CatalogManagerExternalResource catalogManagerResource,
                                                              MongoDBVariantStorageEngine variantStorageEngine) throws IOException, CatalogException, URISyntaxException, StorageEngineException, ToolException {

        AbstractClinicalManagerTest clinicalTest = new AbstractClinicalManagerTest();

        clinicalTest.catalogManagerResource = catalogManagerResource;
        clinicalTest.setUp();

        // Copy config files in the OpenCGA home conf folder
        Files.createDirectory(catalogManagerResource.getOpencgaHome().resolve("conf"));
        catalogManagerResource.getConfiguration().serialize(
                new FileOutputStream(catalogManagerResource.getOpencgaHome().resolve("conf").resolve("configuration.yml").toString()));

        InputStream storageConfigurationStream = MongoDBVariantStorageTest.class.getClassLoader()
                .getResourceAsStream("storage-configuration.yml");
        Files.copy(storageConfigurationStream, catalogManagerResource.getOpencgaHome().resolve("conf").resolve("storage-configuration.yml"),
                StandardCopyOption.REPLACE_EXISTING);

        ObjectMap storageOptions = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        StorageConfiguration configuration = variantStorageEngine.getConfiguration();
        configuration.getVariant().setDefaultEngine(variantStorageEngine.getStorageEngineId());
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(configuration);
        storageEngineFactory.registerVariantStorageEngine(variantStorageEngine);

        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManagerResource.getCatalogManager(), storageEngineFactory);

        Path outDir = Paths.get("target/test-data").resolve("junit_clinical_analysis_" + RandomStringUtils.randomAlphabetic(10));
        Files.createDirectories(outDir);

        variantStorageManager.index(clinicalTest.studyFqn, "family.vcf", outDir.toString(), storageOptions, clinicalTest.token);

        return clinicalTest;
    }

    public static void displayClinicalVariants(List<ClinicalVariant> clinicalVariants, String msg) {
        System.out.println(msg);
        if (CollectionUtils.isNotEmpty(clinicalVariants)) {
            System.out.println("\tNum. clinical variants = " + clinicalVariants.size());
            for (ClinicalVariant variant : clinicalVariants) {
                System.out.println("\t\tclinical variant = " + variant.toStringSimple());
                System.out.println("\t\t\t\tNum. clinical variant evidences = " + variant.getEvidences().size());
                for (ClinicalVariantEvidence clinicalVariantEvidence : variant.getEvidences()) {
                    System.out.print("\t\t\t\t\t(Tier, CT) = (" + clinicalVariantEvidence.getClassification().getTier() + ", ");
                    if (CollectionUtils.isEmpty(clinicalVariantEvidence.getGenomicFeature().getConsequenceTypes())) {
                        System.out.print("EMPTY");
                    } else {
                        System.out.print(clinicalVariantEvidence.getGenomicFeature().getConsequenceTypes().stream().map(SequenceOntologyTerm::getName)
                                .collect(Collectors.joining(",")));
                    }
                    System.out.println(")");
                }
            }
        } else {
            System.out.println("\tNum. variants = 0");
        }
    }
}
