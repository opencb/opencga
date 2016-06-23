package org.opencb.opencga.analysis.execution.plugins.ibs;

import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.beans.Analysis;
import org.opencb.opencga.analysis.beans.Execution;
import org.opencb.opencga.analysis.beans.Option;
import org.opencb.opencga.analysis.execution.plugins.OpenCGAAnalysis;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 26/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IbsAnalysis extends OpenCGAAnalysis {

    public static final String OUTDIR = "outdir";
    public static final String PLUGIN_ID = "ibs_plugin";
    private final Analysis manifest;

    public IbsAnalysis() {
        List<Option> validParams = Arrays.asList(
                new Option(OUTDIR, "", true)
        );
        List<Execution> executions = Collections.singletonList(
                new Execution("default", "default", "", Collections.emptyList(), Collections.emptyList(), OUTDIR, 
                        validParams, Collections.emptyList(), null, null)
        );
        manifest = new Analysis(null, "0.1.0", PLUGIN_ID, "IBS plugin", "", "", "", null, Collections.emptyList(), executions, null, null);
    }

    @Override
    public Analysis getManifest() {
        return manifest;
    }

    @Override
    public String getIdentifier() {
        return PLUGIN_ID;
    }

    @Override
    public int run() throws Exception {

        CatalogManager catalogManager = getCatalogManager();
        String sessionId = getSessionId();
        long studyId = getStudyId();
        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId);

        IdentityByStateClustering ibsc = new IdentityByStateClustering();
        List<String> samples = catalogManager
                .getAllSamples(studyId, new Query(), new QueryOptions(), sessionId)
                .getResult()
                .stream()
                .map(Sample::getName)
                .collect(Collectors.toList());

        Query query = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId);

        List<IdentityByState> identityByStateList = ibsc.countIBS(dbAdaptor.iterator(query, null), samples);
        String outdir = getConfiguration().getString(OUTDIR);
        Path outfile = Paths.get(outdir).resolve(String.valueOf(studyId) + ".genome.gz");

        try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(outfile.toFile()))) {
            ibsc.write(outputStream, identityByStateList, samples);
        }

        return 0;
    }

}
