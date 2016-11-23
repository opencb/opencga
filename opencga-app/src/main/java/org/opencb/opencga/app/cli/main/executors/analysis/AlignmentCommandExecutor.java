package org.opencb.opencga.app.cli.main.executors.analysis;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.tools.alignment.converters.SAMRecordToAvroReadAlignmentConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.analysis.options.AlignmentCommandOptions;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;

import java.io.IOException;

/**
 * Created by pfurio on 11/11/16.
 */
public class AlignmentCommandExecutor extends OpencgaCommandExecutor {

    private AlignmentCommandOptions alignmentCommandOptions;

    public AlignmentCommandExecutor(AlignmentCommandOptions  alignmentCommandOptions) {
        super(alignmentCommandOptions.analysisCommonOptions);
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
            case "query":
                query();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);

    }

    private QueryResponse index() throws CatalogException, IOException {
        logger.debug("Indexing alignment(s)");

        String fileIds = alignmentCommandOptions.indexAlignmentCommandOptions.fileId;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("studyId", alignmentCommandOptions.indexAlignmentCommandOptions.studyId);
        o.putIfNotNull("outDir", alignmentCommandOptions.indexAlignmentCommandOptions.outdirId);

        return openCGAClient.getAlignmentClient().index(fileIds, o);
    }

    private QueryResponse query() throws CatalogException, IOException {
        logger.debug("Querying alignment(s)");

        String fileIds = alignmentCommandOptions.queryAlignmentCommandOptions.fileId;

        ObjectMap o = new ObjectMap();
//        o.putIfNotNull("studyId", alignmentCommandOptions.queryAlignmentCommandOptions.studyId);
        o.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), alignmentCommandOptions.queryAlignmentCommandOptions.region);
        o.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), alignmentCommandOptions.queryAlignmentCommandOptions.minMappingQuality);
        o.putIfNotNull("limit", alignmentCommandOptions.queryAlignmentCommandOptions.limit);

        QueryResponse<ReadAlignment> queryResponse = openCGAClient.getAlignmentClient().query(fileIds, o);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        try {
//            SAMRecordToAvroReadAlignmentConverter samRecordToAvroReadAlignmentConverter = new SAMRecordToAvroReadAlignmentConverter();
//            for (ReadAlignment readAlignment : queryResponse.allResults()) {
//                System.out.println(samRecordToAvroReadAlignmentConverter.from(readAlignment));
//            }
            System.out.println(objectWriter.writeValueAsString(queryResponse.getResponse()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queryResponse;
    }
}
