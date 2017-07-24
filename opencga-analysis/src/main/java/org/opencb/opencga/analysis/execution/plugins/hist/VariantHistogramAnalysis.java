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

package org.opencb.opencga.analysis.execution.plugins.hist;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.execution.plugins.OpenCGAAnalysis;
import org.opencb.opencga.catalog.models.tool.Execution;
import org.opencb.opencga.catalog.models.tool.Manifest;
import org.opencb.opencga.catalog.models.tool.Option;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.*;

import static java.util.Arrays.asList;

/**
 * Created on 15/03/17.
 *
 * new PluginExecutor(catalogManager, sessionId)
 *      .execute(VariantHistogramAnalysis.class, "default", studyId, params);
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHistogramAnalysis extends OpenCGAAnalysis {

    public static final String PLUGIN_ID = "variant_histogram";
    public static final String OUTDIR = "outdir";
    public static final String FILENAME = "fileName";
    public static final String INTERVAL = "interval";
    private Manifest manifest;

    public VariantHistogramAnalysis() {
        manifest = new Manifest(null, "0.1.0", PLUGIN_ID, PLUGIN_ID, "", "", "", null, Collections.emptyList(),
                asList(
                        new Execution("default", "default", "",
                                Collections.emptyList(),
                                Collections.emptyList(),
                                OUTDIR,
                                asList(
                                        new Option(OUTDIR, "", true),
                                        new Option(FILENAME, "", false),
                                        new Option(INTERVAL, "", false)
                                ), Collections.emptyList(), null, null)
                ), null, null);
    }

    @Override
    public String getIdentifier() {
        return PLUGIN_ID;
    }

    @Override
    public Manifest getManifest() {
        return manifest;
    }

    @Override
    public int run(Map<String, Path> input, Path outdir, ObjectMap params) throws Exception {
        Query query = VariantStorageManager.getVariantQuery(params);
        String fileName = params.getString(FILENAME);
        return run(query, params.getInt(INTERVAL, 1000), outdir, fileName);
    }

    protected int run(Query query, int interval, Path outdir, String fileName) throws Exception {
        //ParallelTaskRunner<Variant, Pair<Region, Integer>> ?

        Region region = new Region("", 0, 0);
        List<Variant> variants = new ArrayList<>();
        PrintStream out;
        File file = outdir.toAbsolutePath().toFile();
        boolean stdout = file.isDirectory() && StringUtils.isEmpty(fileName);
        if (stdout) {
            out = System.out;
        } else {
            if (StringUtils.isNotEmpty(fileName)) {
                file = outdir.resolve(fileName).toFile();
            }
            out = new PrintStream(new BufferedOutputStream(new FileOutputStream(file)));
        }
        try {
            out.println("#CHR\tSTART\tEND\tCOUNT");
            QueryOptions options = new QueryOptions(QueryOptions.SORT, true)
                    .append(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES, VariantField.ANNOTATION));
            getVariantStorageManager().iterable(getSessionId()).forEach(query, variant -> {
                if (checkVariant(variant)) {
                    if (region.overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd())) {
                        variants.add(variant);
                    } else {
                        if (!variants.isEmpty()) {
                            out.print(region.getChromosome());
                            out.print('\t');
                            out.print(region.getStart());
                            out.print('\t');
                            out.print(region.getEnd());
                            out.print('\t');
                            out.print(variants.size());
                            out.println();
                        }
                        region.setChromosome(variant.getChromosome());
                        region.setStart(variant.getStart() / interval * interval);
                        region.setEnd(region.getStart() + interval);
                        variants.clear();
                        variants.add(variant);
                    }
                }
            }, options);
        } finally {
            if (!stdout) {
                out.close();
            }
        }
        return 0;
    }

    private boolean checkVariant(Variant variant) {
        return true;
    }

}
