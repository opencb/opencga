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

package org.opencb.opencga.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.analysis.variant.gwas.GwasExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.nio.file.Path;
import java.util.List;

@AnalysisExecutor(id = "HBaseIter", analysis = "GWAS", source = AnalysisExecutor.Source.HBASE, framework = AnalysisExecutor.Framework.MAP_REDUCE)
public class GwasHBaseIterAnalysisExecutor extends GwasExecutor {

    public GwasHBaseIterAnalysisExecutor() {
    }

    public GwasHBaseIterAnalysisExecutor(List<String> list1, List<String> list2, ObjectMap params, Path outDir, GwasConfiguration configuration) {
        super(list1, list2, params, outDir, configuration);
    }


    @Override
    public void exec() {
        System.out.println("This class must be moved to opencga-storage-hadoop");
    }
}
