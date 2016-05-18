package org.opencb.opencga.analysis.executors;

import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;

import java.io.IOException;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface ExecutorManager {

    void execute(Job job, String sessionId) throws CatalogException, AnalysisExecutionException;

    // String status(Job job, String sessionId);

    // Job kill(Job job, String sessionId);

}
