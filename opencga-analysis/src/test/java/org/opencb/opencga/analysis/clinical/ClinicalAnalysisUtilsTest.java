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
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractClinicalManagerTest;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalAnalysisUtilsTest {

    public static AbstractClinicalManagerTest getClinicalTest(OpenCGATestExternalResource opencga) throws IOException, CatalogException, URISyntaxException, StorageEngineException, ToolException {

        AbstractClinicalManagerTest clinicalTest = new AbstractClinicalManagerTest();

        clinicalTest.catalogManagerResource = opencga.getCatalogManagerExternalResource();
        clinicalTest.setUp();

        // Exomiser analysis
        Path exomiserDataPath = opencga.getOpencgaHome().resolve("analysis/exomiser");
        Files.createDirectories(exomiserDataPath);
        Path parent = Paths.get(ClinicalAnalysisUtilsTest.class.getClassLoader().getResource("pheno").getPath()).getParent();
        Files.copy(parent.resolve("exomiser/application.properties"), exomiserDataPath.resolve("application.properties"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(parent.resolve("exomiser/output.yml"), exomiserDataPath.resolve("output.yml"),
                StandardCopyOption.REPLACE_EXISTING);

        // Storage
        ObjectMap storageOptions = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), false);

        VariantStorageManager variantStorageManager = new VariantStorageManager(opencga.getCatalogManager(), opencga.getStorageEngineFactory());

        Path outDir = Paths.get("target/test-data").resolve("junit_clinical_analysis_" + RandomStringUtils.randomAlphabetic(10));
        Files.createDirectories(outDir);

        variantStorageManager.index(clinicalTest.studyFqn, "family.vcf", outDir.toString(), storageOptions, clinicalTest.token);
        variantStorageManager.index(clinicalTest.studyFqn, "exomiser.vcf.gz", outDir.toString(), storageOptions, clinicalTest.token);

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
