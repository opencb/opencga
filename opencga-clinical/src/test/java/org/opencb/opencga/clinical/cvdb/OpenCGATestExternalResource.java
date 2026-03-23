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

package org.opencb.opencga.clinical.cvdb;

import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;

import java.nio.file.Paths;

/**
 * Created on 26/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OpenCGATestExternalResource extends OpenCGATestExternalResource {

    public OpenCGATestExternalResource() {
        this(false);
    }

    public OpenCGATestExternalResource(boolean storageHadoop) {
        super(storageHadoop, Paths.get("../opencga-home/opencga-app/app/analysis/"));
    }
}
