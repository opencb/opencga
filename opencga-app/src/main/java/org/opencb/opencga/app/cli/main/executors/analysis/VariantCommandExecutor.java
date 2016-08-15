package org.opencb.opencga.app.cli.main.executors.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.analysis.VariantCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by pfurio on 15/08/16.
 */
public class VariantCommandExecutor extends OpencgaCommandExecutor {

    private VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        switch (subCommandString) {
            case "index":
                createOutput(index());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private QueryResponse index() throws CatalogException, IOException {
        logger.debug("Indexing variant(s)");

        String fileIds = variantCommandOptions.indexCommandOptions.fileIds;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("studyId", variantCommandOptions.indexCommandOptions.studyId);
        o.putIfNotNull("outDir", variantCommandOptions.indexCommandOptions.outdirId);
        o.putIfNotNull("transform", variantCommandOptions.indexCommandOptions.transform);
        o.putIfNotNull("load", variantCommandOptions.indexCommandOptions.load);
        o.putIfNotNull("excludeGenotypes", variantCommandOptions.indexCommandOptions.excludeGenotype);
        o.putIfNotNull("includeExtraFields", variantCommandOptions.indexCommandOptions.extraFields);
        o.putIfNotNull("aggregated", variantCommandOptions.indexCommandOptions.aggregated);
        o.putIfNotNull("calculateStats", variantCommandOptions.indexCommandOptions.calculateStats);
        o.putIfNotNull("annotate", variantCommandOptions.indexCommandOptions.annotate);
//        o.putIfNotNull("overwrite", variantCommandOptions.indexCommandOptions.overwriteAnnotations);

        return openCGAClient.getFileClient().index(fileIds, o);
    }

}
