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

package org.opencb.opencga.analysis.individual.qc;

import org.junit.Test;
import org.opencb.biodata.models.clinical.qc.RelatednessReport;
import org.opencb.opencga.analysis.family.qc.IBDComputation;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

public class IndividualQcUtilsTest {

    @Test
    public void buildRelatednessReport() throws ToolException, IOException {

        URI resourceUri = getResourceUri("ibd.genome");
        File file = Paths.get(resourceUri.getPath()).toFile();
        List<RelatednessReport.RelatednessScore> relatednessReport = IBDComputation.parseRelatednessScores(file);

        System.out.println(JacksonUtils.getDefaultNonNullObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(relatednessReport));
    }
}