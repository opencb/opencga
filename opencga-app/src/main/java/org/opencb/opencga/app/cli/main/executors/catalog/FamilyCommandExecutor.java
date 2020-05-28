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

package org.opencb.opencga.app.cli.main.executors.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.FamilyCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyAclUpdateParams;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.family.IndividualCreateParams;
import org.opencb.opencga.core.response.RestResponse;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 15/05/17.
 */
public class FamilyCommandExecutor extends OpencgaCommandExecutor {

    private FamilyCommandOptions familyCommandOptions;

    public FamilyCommandExecutor(FamilyCommandOptions familyCommandOptions) {
        super(familyCommandOptions.commonCommandOptions);
        this.familyCommandOptions = familyCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing family command line");

        String subCommandString = getParsedSubCommand(familyCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
            case "stats":
                queryResponse = stats();
                break;
//            case "update":
//                queryResponse = update();
//                break;
            case "acl":
                queryResponse = acl();
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "annotation-sets-update":
                queryResponse = updateAnnotations();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }


    private RestResponse<Family> create() throws ClientException {
        logger.debug("Creating a new family");

        FamilyCommandOptions.CreateCommandOptions commandOptions = familyCommandOptions.createCommandOptions;

        FamilyCreateParams data = new FamilyCreateParams()
                .setId(commandOptions.id)
                .setName(commandOptions.name)
                .setDescription(commandOptions.description);

        if (commandOptions.members != null) {
            data.setMembers(commandOptions.members
                    .stream()
                    .map(memberId -> new IndividualCreateParams().setId(memberId))
                    .collect(Collectors.toList())
            );
        }

        ObjectMap params = new ObjectMap();
        params.put(FamilyDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        return openCGAClient.getFamilyClient().create(data, params);
    }

    private RestResponse<Family> info() throws ClientException {
        logger.debug("Getting family information");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.STUDY.key(), familyCommandOptions.infoCommandOptions.study);
        params.putIfNotNull(QueryOptions.INCLUDE, familyCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, familyCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        params.put("flattenAnnotations", familyCommandOptions.infoCommandOptions.flattenAnnotations);
        return openCGAClient.getFamilyClient().info(familyCommandOptions.infoCommandOptions.family, params);
    }

    private RestResponse<Family> search() throws ClientException {
        FamilyCommandOptions.SearchCommandOptions commandOptions = familyCommandOptions.searchCommandOptions;

        logger.debug("Searching family");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.ID.key(), commandOptions.name);
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.MEMBERS.key(), commandOptions.members);
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.ANNOTATION.key(), commandOptions.annotation);
        params.putIfNotNull(FamilyDBAdaptor.QueryParams.MEMBERS_PARENTAL_CONSANGUINITY.key(), commandOptions.parentalConsanguinity);
        params.put("flattenAnnotations", commandOptions.flattenAnnotations);
        params.putAll(commandOptions.commonOptions.params);
        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);

        return openCGAClient.getFamilyClient().search(params);
    }

    private RestResponse<FacetField> stats() throws ClientException {
        logger.debug("Family stats");

        FamilyCommandOptions.StatsCommandOptions commandOptions = familyCommandOptions.statsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        params.putIfNotEmpty("creationYear", commandOptions.creationYear);
        params.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        params.putIfNotEmpty("creationDay", commandOptions.creationDay);
        params.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        params.putIfNotEmpty("phenotypes", commandOptions.phenotypes);
        params.putIfNotEmpty("status", commandOptions.status);
        params.putIfNotEmpty("numMembers", commandOptions.numMembers);
        params.putIfNotEmpty("release", commandOptions.release);
        params.putIfNotEmpty("version", commandOptions.version);
        params.putIfNotEmpty("expectedSize", commandOptions.expectedSize);
        params.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);

        params.put("default", commandOptions.defaultStats);
        params.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getFamilyClient().aggregationStats(params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = familyCommandOptions.aclsUpdateCommandOptions;

        FamilyAclUpdateParams updateParams = new FamilyAclUpdateParams()
                .setFamily(extractIdsFromListOrFile(commandOptions.id))
                .setPermissions(commandOptions.permissions);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", commandOptions.study);

        return openCGAClient.getFamilyClient().updateAcl(commandOptions.memberId, commandOptions.action.name(), updateParams, params);
    }

    private RestResponse<Family> updateAnnotations() throws ClientException, IOException {
        AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions commandOptions = familyCommandOptions.annotationUpdateCommandOptions;

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap annotations = mapper.readValue(new File(commandOptions.annotations), ObjectMap.class);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
//        queryParams.putIfNotNull("action", updateCommandOptions.action);

        return openCGAClient.getFamilyClient().updateAnnotations(commandOptions.id, commandOptions.annotationSetId, annotations, params);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = familyCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getFamilyClient().acl(commandOptions.id, params);
    }


}
