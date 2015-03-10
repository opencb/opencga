package org.opencb.opencga.analysis.storage.variant;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.exec.Command;
import org.opencb.opencga.lib.exec.SingleProcess;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by hpccoll1 on 06/03/15.
 */
public class VariantStorage {

    final CatalogManager catalogManager;

    public VariantStorage(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }


    public QueryResult<Job> calculateStats(int indexFileId, int cohortId, String sessionId, QueryOptions options) throws CatalogException {

        File indexFile = catalogManager.getFile(indexFileId, sessionId).first();
        Cohort cohort = catalogManager.getCohort(cohortId, null, sessionId).first();

        if (indexFile.getType() != File.Type.INDEX || indexFile.getBioformat() != File.Bioformat.VARIANT) {
            throw new CatalogException("Expected file with {type: INDEX, bioformat: VARIANT}. " +
                    "Got {type: " + indexFile.getType() + ", bioformat: " + indexFile.getBioformat() + "}");
        }
        int studyId = catalogManager.getStudyIdByFileId(indexFile.getId());

        QueryResult<Sample> sampleQueryResult = catalogManager.getAllSamples(studyId, new QueryOptions("id", cohort.getSamples()), sessionId);
        int outDirId = catalogManager.getFileParent(indexFileId, null, sessionId).first().getId();

        /** Create temporal Job Outdir **/
        String randomString = "I_" + StringUtils.randomString(10);
        URI temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);

        /** create command line **/
        String opencgaStorageBinName = "opencga-storage.sh";
        String opencgaStorageBinPath = Paths.get(Config.getOpenCGAHome(), "bin", opencgaStorageBinName).toString();

        StringBuilder sb = new StringBuilder()

                .append(opencgaStorageBinPath)
                .append(" --storage-engine ").append(indexFile.getAttributes().get(AnalysisFileIndexer.STORAGE_ENGINE))
                .append(" stats-variants ")
                .append(" --file-id ").append(indexFile.getId())
                .append(" --output-filename ").append(temporalOutDirUri.resolve("stats." + cohort.getName()).toString())
                .append(" --study-id ").append(studyId)
                .append(" --database ").append(indexFile.getAttributes().get(AnalysisFileIndexer.DB_NAME))
                .append(" --cohort-name ").append(cohort.getId())
                .append(" --cohort-samples ")
                ;
        for (Sample sample : sampleQueryResult.getResult()) {
            sb.append(sample.getName()).append(",");
        }

        String commandLine = sb.toString();
        System.out.println("commandLine = " + commandLine);

        String jobDescription = "Stats calculation for cohort " + cohortId;
        String jobName = "calculate-stats";
        QueryResult<Job> jobQueryResult;
        if (options.getBoolean("execute")) {

//            URI out = temporalOutDirUri.resolve("job_out." + job.getId() + ".log");
//            URI err = temporalOutDirUri.resolve("job_err." + job.getId() + ".log");

            Command com = new Command(commandLine);
            SingleProcess sp = new SingleProcess(com);
            sp.getRunnableProcess().run();
            jobQueryResult = catalogManager.createJob(studyId, jobName, opencgaStorageBinName, jobDescription, commandLine, temporalOutDirUri,
                    outDirId, Collections.<Integer>emptyList(), null, Job.Status.DONE, null, sessionId);

        } else {
            jobQueryResult = catalogManager.createJob(studyId, jobName, opencgaStorageBinName, jobDescription, commandLine, temporalOutDirUri,
                    outDirId, Collections.<Integer>emptyList(), null, options, sessionId);
        }

        return jobQueryResult;
    }

}
