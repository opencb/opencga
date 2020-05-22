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

package org.opencb.opencga.analysis.variant.circos;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.BreakendMate;
import org.opencb.biodata.models.variant.avro.StructuralVariantType;
import org.opencb.biodata.models.variant.avro.StructuralVariation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.CircosAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;

@ToolExecutor(id="opencga-local", tool = CircosAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL, source = ToolExecutor.Source.STORAGE)
public class CircosLocalAnalysisExecutor extends CircosAnalysisExecutor implements VariantStorageToolExecutor {

    public final static String R_DOCKER_IMAGE = "opencb/opencga-r:2.0.0-dev";

    private File snvsFile;
    private File rearrsFile;
    private File indelsFile;
    private File cnvsFile;

    @Override
    public void run() throws ToolException, IOException {
        // Create files
        snvsFile = getOutDir().resolve("snvs.tsv").toFile();
        PrintWriter snvsPw = new PrintWriter(snvsFile);
        snvsPw.println("Chromosome\tchromStart\tchromEnd\tref\talt\tlogDistPrev");

        rearrsFile = getOutDir().resolve("rearrs.tsv").toFile();
        PrintWriter rearrsPw = new PrintWriter(rearrsFile);
        rearrsPw.println("Chromosome\tchromStart\tchromEnd\tChromosome.1\tchromStart.1\tchromEnd.1\ttype");

        indelsFile = getOutDir().resolve("indels.tsv").toFile();
        PrintWriter indelsPw = new PrintWriter(indelsFile);
        indelsPw.println("Chromosome\tchromStart\tchromEnd\ttype\tclassification");

        cnvsFile = getOutDir().resolve("cnvs.tsv").toFile();
        PrintWriter cnvsPw = new PrintWriter(cnvsFile);
        cnvsPw.println("Chromosome\tchromStart\tchromEnd\tlabel\tmajorCopyNumber\tminorCopyNumber");

        // Launch a thread per query
        VariantStorageManager storageManager = getVariantStorageManager();

        ExecutorService threadPool = Executors.newFixedThreadPool(4);

        List<Future<Boolean>> futureList = new ArrayList<>(4);
        futureList.add(threadPool.submit(getNamedThread("SNV", ()
                -> snvQuery(getQuery(), storageManager, snvsPw))));
        futureList.add(threadPool.submit(getNamedThread("COPY_NUMBER", ()
                -> copyNumberQuery(getExecutorParams().getBoolean("plotCopyNumber") ? getQuery() : null, storageManager, cnvsPw))));
        futureList.add(threadPool.submit(getNamedThread("INDEL", ()
                -> indelQuery(getExecutorParams().getBoolean("plotIndels") ? getQuery() : null, storageManager, indelsPw))));
        futureList.add(threadPool.submit(getNamedThread("REARRANGEMENT", ()
                -> rearrangementQuery(getExecutorParams().getBoolean("plotRearrangements") ? getQuery() : null, storageManager,
                rearrsPw))));

        threadPool.shutdown();

        try {
            threadPool.awaitTermination(2, TimeUnit.MINUTES);
            if (!threadPool.isTerminated()) {
                for (Future<Boolean> future : futureList) {
                    future.cancel(true);
                }
            }
        } catch (InterruptedException e) {
            throw new ToolException("Error launching threads when executing the Cisco analysis", e);
        }

        // Close files
        cnvsPw.close();
        indelsPw.close();
        rearrsPw.close();
        snvsPw.close();

        // Execute R script
        // circos.R ./snvs.tsv ./indels.tsv ./cnvs.tsv ./rearrs.tsv SampleId
        String rScriptPath = getExecutorParams().getString("opencgaHome") + "/analysis/R/" + getToolId();
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        inputBindings.add(new AbstractMap.SimpleEntry<>(rScriptPath, "/data/input"));
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                "/data/output");
        String scriptParams = "R CMD Rscript --vanilla /data/input/circos.R"
                + (getExecutorParams().getBoolean("plotCopyNumber") ? "" : " --no_copynumber")
                + (getExecutorParams().getBoolean("plotIndels") ? "" : " --no_indels")
                + (getExecutorParams().getBoolean("plotRearrangements") ? "" : " --no_rearrangements")
                + " --out_path /data/output/"
                + " /data/output/" + snvsFile.getName()
                + " /data/output/" + indelsFile.getName()
                + " /data/output/" + cnvsFile.getName()
                + " /data/output/" + rearrsFile.getName()
                + " " + getQuery().getString(SAMPLE.key());

        String cmdline = DockerUtils.run(R_DOCKER_IMAGE, inputBindings, outputBinding, scriptParams, null);
        System.out.println("Docker command line: " + cmdline);
    }

    private boolean snvQuery(Query query, VariantStorageManager storageManager, PrintWriter pw) {
        try {
            Query snvQuery = new Query(query);
            snvQuery.put("type", "SNV");
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.INCLUDE, "id")
                    .append(QueryOptions.SORT, true);
            VariantDBIterator iterator = storageManager.iterator(snvQuery, queryOptions, getToken());

            int prevStart = 0;
            String currentChrom = "";
            while (iterator.hasNext()) {
                Variant v = iterator.next();
                if (v.getStart() > v.getEnd()) {
                    // Sanity check
                    addWarning("Skipping variant " + v.toString() + ", start = " + v.getStart() + ", end = " + v.getEnd());
                } else {
                    if (!v.getChromosome().equals(currentChrom)) {
                        prevStart = 0;
                        currentChrom = v.getChromosome();
                    }
                    pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\t"
                            + v.getReference() + "\t" + v.getAlternate() + "\t" + Math.log10(v.getStart() - prevStart));
                    prevStart = v.getStart();
                }
            }
        } catch (Exception e) {
            return false;
//            throw new ToolExecutorException(e);
        }
        return true;
    }

    private boolean copyNumberQuery(Query query, VariantStorageManager storageManager, PrintWriter pw) {
        try {
            if (query != null) {
                Query copyNumberQuery = new Query(query);
                copyNumberQuery.append("type", "CNV");
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,sv");
                VariantDBIterator iterator = storageManager.iterator(copyNumberQuery, queryOptions, getToken());

                while (iterator.hasNext()) {
                    Variant v = iterator.next();
                    StructuralVariation sv = v.getSv();
                    if (sv != null) {
                        if (sv.getType() == StructuralVariantType.COPY_NUMBER_GAIN) {
                            pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\tNONE\t"
                                    + sv.getCopyNumber() + "\t1");
                        } else if (sv.getType() == StructuralVariantType.COPY_NUMBER_LOSS) {
                            pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\tNONE\t"
                                    + "1\t" + sv.getCopyNumber());
                        } else {
                            addWarning("Skipping variant " + v.toString() + ": invalid SV type " + sv.getType() + " for copy-number (CNV)");
                        }
                    } else {
                        addWarning("Skipping variant " + v.toString() + ": SV is empty for copy-number (CNV)");
                    }
                }
            }
        } catch (Exception e) {
            return false;
//            throw new ToolExecutorException(e);
        }
        return true;
    }

    private boolean indelQuery(Query query, VariantStorageManager storageManager, PrintWriter pw) {
        try {
            if (query != null) {
                Query indelQuery = new Query(query);
                indelQuery.append("type", "INSERTION,DELETION,INDEL");
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id");
                VariantDBIterator iterator = storageManager.iterator(indelQuery, queryOptions, getToken());

                while (iterator.hasNext()) {
                    Variant v = iterator.next();
                    switch (v.getType()) {
                        case INSERTION: {
                            pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\tI\tNone");
                            break;
                        }
                        case DELETION: {
                            pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\tD\tNone");
                            break;
                        }
                        case INDEL: {
                            pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\tDI\tNone");
                            break;
                        }
                        default: {
                            // Sanity check
                            addWarning("Skipping variant " + v.toString() + ": invalid type " + v.getType()
                                    + " for INSERTION, DELETION, INDEL");
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            return false;
//            throw new ToolExecutorException(e);
        }
        return true;
    }

    private boolean rearrangementQuery(Query query, VariantStorageManager storageManager, PrintWriter pw) {
        try {
            if (query != null) {
                Query rearrangementQuery = new Query(query);
                rearrangementQuery.append("type", "DELETION,TRANSLOCATION,INVERSION,DUPLICATION,BREAKEND");
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,sv");
                VariantDBIterator iterator = storageManager.iterator(rearrangementQuery, queryOptions, getToken());

                while (iterator.hasNext()) {
                    Variant v = iterator.next();
                    String type = null;
                    switch (v.getType()) {
                        case DELETION: {
                            type = "DEL";
                            break;
                        }
                        case BREAKEND:
                        case TRANSLOCATION: {
                            type = "BND";
                            break;
                        }
                        case DUPLICATION: {
                            type = "DUP";
                            break;
                        }
                        case INVERSION: {
                            type = "INV";
                            break;
                        }
                        default: {
                            // Sanity check
                            addWarning("Skipping variant " + v.toString() + ": invalid type " + v.getType() + " for rearrangement");
                            break;
                        }
                    }

                    if (type != null) {
                        // Check structural variation
                        StructuralVariation sv = v.getSv();
                        if (sv != null) {
                            if (sv.getBreakend() != null) {
                                if (sv.getBreakend().getMate() != null) {
                                    BreakendMate mate = sv.getBreakend().getMate();
                                    pw.println(v.getChromosome() + "\t" + v.getStart() + "\t" + v.getEnd() + "\t" + mate.getChromosome()
                                            + "\t" + mate.getPosition() + "\t" + mate.getPosition() + "\t" + type);
                                } else {
                                    addWarning("Skipping variant " + v.toString() + ": " + v.getType() + ", breakend mate is empty for"
                                            + " rearrangement");
                                }
                            } else {
                                addWarning("Skipping variant " + v.toString() + ": " + v.getType() + ", breakend is empty for"
                                        + " rearrangement");
                            }
                        } else {
                            addWarning("Skipping variant " + v.toString() + ": " + v.getType() + ", SV is empty for rearrangement");
                        }
                    }
                }
            }
        } catch (Exception e) {
            return false;
//            throw new ToolExecutorException(e);
        }
        return true;
    }

    private <T> Callable<T> getNamedThread(String name, Callable<T> c) {
        String parentThreadName = Thread.currentThread().getName();
        return () -> {
            Thread.currentThread().setName(parentThreadName + "-" + name);
            return c.call();
        };
    }
}
