package org.opencb.opencga.app.cli.main.parent;

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ExecutionsCommandOptions;
import org.opencb.opencga.app.cli.main.utils.ExecutionsTopManager;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.ExecutionTop;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;

public abstract class ParentExecutionsCommandExecutor extends OpencgaCommandExecutor {

    private final ExecutionsCommandOptions executionsCommandOptions;

    public ParentExecutionsCommandExecutor(GeneralCliOptions.CommonCommandOptions options,
                                           ExecutionsCommandOptions executionsCommandOptions) throws CatalogAuthenticationException {
        super(options);
        this.executionsCommandOptions = executionsCommandOptions;
    }

    protected RestResponse<ExecutionTop> top() throws Exception {
        ExecutionsCommandOptions.TopCommandOptions c = executionsCommandOptions.topCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), c.study);
        query.putIfNotEmpty(ParamConstants.JOB_TOOL_ID_PARAM, c.toolId);
        query.putIfNotEmpty(ParamConstants.INTERNAL_STATUS_PARAM, c.internalStatus);
        query.putIfNotEmpty(ParamConstants.JOB_USER_PARAM, c.userId);
        query.putIfNotEmpty(ParamConstants.JOB_PRIORITY_PARAM, c.priority);
        query.putAll(c.commonOptions.params);

        new ExecutionsTopManager(openCGAClient, query, c.iterations, c.executionsLimit, c.delay, c.plain, c.columns).run();
        RestResponse<ExecutionTop> res = new RestResponse<>();
        res.setType(QueryType.VOID);
        return res;
    }

}
