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

package org.opencb.opencga.app.cli.main.io;

import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;

/**
 * Created by pfurio on 28/07/16.
 */
public class JsonOutputWriter extends AbstractOutputWriter {

    public JsonOutputWriter() {
        super();
    }

    public JsonOutputWriter(WriterConfiguration writerConfiguration) {
        super(writerConfiguration);
    }

    @Override
    public void print(RestResponse queryResponse) {
        if (checkErrors(queryResponse)) {
            return;
        }

        ObjectWriter objectWriter;
        if (writerConfiguration.isPretty()) {
            objectWriter = JacksonUtils.getExternalOpencgaObjectMapper().writerWithDefaultPrettyPrinter();
        } else {
            objectWriter = JacksonUtils.getExternalOpencgaObjectMapper().writer();
        }
        Object toPrint = queryResponse;
        if (!writerConfiguration.isMetadata()) {
            toPrint = queryResponse.getResponses();
        }
        try {
            ps.println(objectWriter.writeValueAsString(toPrint));
        } catch (IOException e) {
            CommandLineUtils.error("Could not parse the queryResponse to print as "
                    + (writerConfiguration.isPretty() ? "a beautiful" : "") + " JSON", e);
        }
    }
}
