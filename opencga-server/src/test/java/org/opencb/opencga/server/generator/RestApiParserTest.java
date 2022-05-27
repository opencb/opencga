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

package org.opencb.opencga.server.generator;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.api.RestApiParser;
import org.opencb.commons.api.models.RestApi;
import org.opencb.commons.api.models.RestEndpoint;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class RestApiParserTest {

    private static RestApiParser restApiParser;


    @BeforeClass
    public static void beforeClass() throws Exception {
        restApiParser = new RestApiParser();
    }

    @Test
    public void parse() {
        RestApi parse = restApiParser.parse(VariantOperationWebService.class, true);
        List<RestEndpoint> create = parse.getCategories().get(0).getEndpoints().stream()
                .filter(endpoint -> endpoint.getPath().contains("configure"))
                .collect(Collectors.toList());
        System.out.println("configure = " + create);
    }

    @Test
    public void testParse() {
    }

    @Test
    public void parseToFile() throws IOException {
        List<Class<?>> classes = new LinkedList<>();
        classes.add(UserWSServer.class);
        classes.add(ProjectWSServer.class);
        classes.add(StudyWSServer.class);
        classes.add(FileWSServer.class);
        classes.add(JobWSServer.class);
        classes.add(SampleWSServer.class);
        classes.add(IndividualWSServer.class);
        classes.add(FamilyWSServer.class);
        classes.add(CohortWSServer.class);
        classes.add(PanelWSServer.class);
        classes.add(AlignmentWebService.class);
        classes.add(VariantWebService.class);
        classes.add(ClinicalWebService.class);
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(Ga4ghWSServer.class);
        classes.add(AdminWSServer.class);
        Files.createDirectories(Paths.get("target/test-data/"));
        Path path = Paths.get("target/test-data/restApi.json");
        restApiParser.parseToFile(classes, path);
        System.out.println("path.toAbsolutePath().toString() = " + path.toAbsolutePath());
    }


}