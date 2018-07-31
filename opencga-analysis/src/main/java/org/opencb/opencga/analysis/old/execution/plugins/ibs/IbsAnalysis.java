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

package org.opencb.opencga.analysis.old.execution.plugins.ibs;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.old.execution.plugins.OpenCGAAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.catalog.old.models.tool.Execution;
import org.opencb.opencga.catalog.old.models.tool.Manifest;
import org.opencb.opencga.catalog.old.models.tool.Option;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IbsAnalysis extends OpenCGAAnalysis {

    public static final String OUTDIR = "outdir";
    public static final String SAMPLES = "samples";
    public static final String PLUGIN_ID = "ibs_plugin";
    private final Manifest manifest;

    public IbsAnalysis() {
        manifest = new Manifest(null, "0.1.0", PLUGIN_ID, "IBS plugin", "", "", "", null, Collections.emptyList(),
                Collections.singletonList(
                        new Execution("default", "default", "", Collections.emptyList(), Collections.emptyList(), OUTDIR,
                                Arrays.asList(
                                        new Option(OUTDIR, "", true),
                                        new Option(SAMPLES, "", false)
                                ), Collections.emptyList(), null, null)
                ), null, null);
    }

    @Override
    public Manifest getManifest() {
        return manifest;
    }

    @Override
    public String getIdentifier() {
        return PLUGIN_ID;
    }

    @Override
    public int run(Map<String, Path> input, Path outdir, ObjectMap params) throws Exception {
        return 0;
//        CatalogManager catalogManager = getCatalogManager();
//        String sessionId = getSessionId();
//        long studyId = getStudyId();
//
//        IdentityByStateClustering ibsc = new IdentityByStateClustering();
//        List<String> samples;
//        Query query = new Query(VariantQueryParam.STUDY.key(), studyId);
//        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, VariantField.ANNOTATION);
//
//        Query samplesQuery = new Query();
//        if (StringUtils.isNotEmpty(params.getString(SAMPLES))) {
//            String userId = catalogManager.getUserManager().getUserId(sessionId);
//            List<Long> sampleIds = catalogManager.getSampleManager().getIds(params.getAsStringList(SAMPLES), String.valueOf(studyId),
//                    userId)
//                    .getResourceIds();
//            samplesQuery.append(SampleDBAdaptor.QueryParams.UID.key(), sampleIds);
//            query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds);
//        }
//        samples = catalogManager.getSampleManager().get(studyId, samplesQuery, new QueryOptions(), sessionId)
//                .getResult()
//                .stream()
//                .map(Sample::getId)
//                .collect(Collectors.toList());
//
//
//        List<IdentityByState> identityByStateList;
//        try (VariantDBIterator iterator = getVariantStorageManager().iterable(sessionId).iterator(query, options)) {
//            identityByStateList = ibsc.countIBS(iterator, samples);
//        }
//        if ("-".equals(outdir.getFileName().toString())) {
//            ibsc.write(System.out, identityByStateList, samples);
//        } else {
//            Path outfile;
//            if (outdir.toAbsolutePath().toFile().isDirectory()) {
//                String alias = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionId).first().getId();
//                outfile = outdir.resolve(alias + ".genome.gz");
//            } else {
//                outfile = outdir;
//            }
//
//            try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(outfile.toFile()))) {
//                ibsc.write(outputStream, identityByStateList, samples);
//            }
//        }
//
//        return 0;
    }

}
