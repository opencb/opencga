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
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.rest.SampleWSServer;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RestApiParserTest {

    private static RestApiParser restApiParser;


    @BeforeClass
    public static void beforeClass() throws Exception {
        restApiParser = new RestApiParser();
    }

    @Test
    public void parse() {
        RestApi parse = restApiParser.parse(VariantOperationWebService.class);
        List<RestEndpoint> create = parse.getCategories().get(0).getEndpoints().stream()
                .filter(endpoint -> endpoint.getPath().contains("configure"))
                .collect(Collectors.toList());
        System.out.println("configure = " + create);
    }

    @Test
    public void testParse() {
    }

    @Test
    public void parseToFile() {
        try {
            restApiParser.parseToFile(Collections.singletonList(SampleWSServer.class), Paths.get("/tmp/restApi.json"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}