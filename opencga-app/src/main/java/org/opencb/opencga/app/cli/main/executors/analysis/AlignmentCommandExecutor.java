package org.opencb.opencga.app.cli.main.executors.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.analysis.AlignmentCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by pfurio on 11/11/16.
 */
public class AlignmentCommandExecutor extends OpencgaCommandExecutor {

    private AlignmentCommandOptions alignmentCommandOptions;

    public AlignmentCommandExecutor(AlignmentCommandOptions alignmentCommandOptions) {
        super(alignmentCommandOptions.commonCommandOptions);
        this.alignmentCommandOptions = alignmentCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing alignment command line");

        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "index":
                queryResponse = index();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private QueryResponse index() throws CatalogException, IOException {
        logger.debug("Indexing alignment(s)");

        String fileIds = alignmentCommandOptions.indexCommandOptions.fileIds;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("studyId", alignmentCommandOptions.indexCommandOptions.studyId);
        o.putIfNotNull("outDir", alignmentCommandOptions.indexCommandOptions.outdirId);

        return openCGAClient.getAlignmentClient().index(fileIds, o);
    }
}
