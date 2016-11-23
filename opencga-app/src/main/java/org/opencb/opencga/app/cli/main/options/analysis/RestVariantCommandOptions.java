/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.app.cli.main.options.analysis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

/**
 * Created by pfurio on 15/08/16.
 */
@Parameters(commandNames = {"variant"}, commandDescription = "Variant commands")
@Deprecated
public class RestVariantCommandOptions {

    public JCommander jCommander;
    public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions;
    public IndexCommandOptions indexCommandOptions;

    public RestVariantCommandOptions(OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;
        this.indexCommandOptions = new IndexCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index VCF files")
    public class IndexCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file-id"}, description = "Comma separated list of file ids (files or directories)", required = true, arity = 1)
        public String fileIds;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = false, arity = 1)
        public String studyId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load;

        @Parameter(names = {"--outdir"}, description = "Directory where transformed index files will be stored", required = false, arity = 1)
        public String outdirId;

        @Deprecated
        @Parameter(names = {"--exclude-genotypes"}, description = "Index excluding the genotype information")
        public boolean excludeGenotype;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other genotype fields [CSV]")
        public String extraFields;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

//        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF " +
//                "file")
//        public String aggregationMappingFile = null;
//
//        @Parameter(names = {"--gvcf"}, description = "The input file is in gvcf format")
//        public boolean gvcf;
//
//        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
//        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate = false;
//
//        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
//        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

//        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations already present in variants")
//        public boolean overwriteAnnotations;

    }


}
