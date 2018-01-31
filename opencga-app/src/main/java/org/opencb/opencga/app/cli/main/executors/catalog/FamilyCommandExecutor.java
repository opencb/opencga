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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.commons.AnnotationCommandExecutor;
import org.opencb.opencga.app.cli.main.options.FamilyCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.acls.permissions.FamilyAclEntry;

import java.io.IOException;

/**
 * Created by pfurio on 15/05/17.
 */
public class FamilyCommandExecutor extends OpencgaCommandExecutor {

    private FamilyCommandOptions familyCommandOptions;
    private AclCommandExecutor<Family, FamilyAclEntry> aclCommandExecutor;
    private AnnotationCommandExecutor<Family, FamilyAclEntry> annotationCommandExecutor;

    public FamilyCommandExecutor(FamilyCommandOptions familyCommandOptions) {
        super(familyCommandOptions.commonCommandOptions);
        this.familyCommandOptions = familyCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
        this.annotationCommandExecutor = new AnnotationCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing family command line");

        String subCommandString = getParsedSubCommand(familyCommandOptions.jCommander);
        QueryResponse queryResponse = null;
        switch (subCommandString) {
//            case "create":
//                queryResponse = create();
//                break;
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
//            case "update":
//                queryResponse = update();
//                break;
            case "acl":
                queryResponse = aclCommandExecutor.acls(familyCommandOptions.aclsCommandOptions, openCGAClient.getFamilyClient());
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            case "annotation-sets-create":
                queryResponse = annotationCommandExecutor.createAnnotationSet(familyCommandOptions.annotationCreateCommandOptions,
                        openCGAClient.getFamilyClient());
                break;
            case "annotation-sets-search":
                queryResponse = annotationCommandExecutor.searchAnnotationSets(familyCommandOptions.annotationSearchCommandOptions,
                        openCGAClient.getFamilyClient());
                break;
            case "annotation-sets-delete":
                queryResponse = annotationCommandExecutor.deleteAnnotationSet(familyCommandOptions.annotationDeleteCommandOptions,
                        openCGAClient.getFamilyClient());
                break;
            case "annotation-sets":
                queryResponse = annotationCommandExecutor.getAnnotationSet(familyCommandOptions.annotationInfoCommandOptions,
                        openCGAClient.getFamilyClient());
                break;
            case "annotation-sets-update":
                queryResponse = annotationCommandExecutor.updateAnnotationSet(familyCommandOptions.annotationUpdateCommandOptions,
                        openCGAClient.getFamilyClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }


//    private QueryResponse<Family> create() throws CatalogException, IOException {
//        logger.debug("Creating a new family");
//
//        String studyId = resolveStudy(familyCommandOptions.createCommandOptions.study);
//
//        ObjectMap params = new ObjectMap();
//        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.NAME.key(), familyCommandOptions.createCommandOptions.name);
//        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.DESCRIPTION.key(), familyCommandOptions.createCommandOptions.description);
//        params.put(FamilyDBAdaptor.QueryParams.MEMBERS_PARENTAL_CONSANGUINITY.key(),
//                familyCommandOptions.createCommandOptions.parentalConsanguinity);
//        if (StringUtils.isNotEmpty(familyCommandOptions.createCommandOptions.father)) {
//            params.put(FamilyDBAdaptor.QueryParams.FATHER.key(), new ObjectMap("name", familyCommandOptions.createCommandOptions.father));
//        }
//        if (StringUtils.isNotEmpty(familyCommandOptions.createCommandOptions.mother)) {
//            params.put(FamilyDBAdaptor.QueryParams.MOTHER.key(), new ObjectMap("name", familyCommandOptions.createCommandOptions.mother));
//        }
//        if (StringUtils.isNotEmpty(familyCommandOptions.createCommandOptions.children)) {
//            List<String> childrenNameList = Arrays.asList(StringUtils.split(familyCommandOptions.createCommandOptions.children, ","));
//            List<ObjectMap> childrenList = new ArrayList<>(childrenNameList.size());
//            for (String name : childrenNameList) {
//                childrenList.add(new ObjectMap("name", name));
//            }
//            params.put(FamilyDBAdaptor.QueryParams.MEMBER.key(), childrenList);
//        }
//
//        return openCGAClient.getFamilyClient().create(studyId, params);
//    }

    private QueryResponse<Family> info() throws CatalogException, IOException {
        logger.debug("Getting family information");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.STUDY.key(), resolveStudy(familyCommandOptions.infoCommandOptions.study));
        params.putIfNotNull(QueryOptions.INCLUDE, familyCommandOptions.infoCommandOptions.dataModelOptions.include);
        params.putIfNotNull(QueryOptions.EXCLUDE, familyCommandOptions.infoCommandOptions.dataModelOptions.exclude);
        return openCGAClient.getFamilyClient().get(familyCommandOptions.infoCommandOptions.family, params);
    }

    private QueryResponse<Family> search() throws CatalogException, IOException {
        logger.debug("Searching family");

        Query query = new Query();
        query.putIfNotEmpty(FamilyDBAdaptor.QueryParams.STUDY.key(),
                resolveStudy(familyCommandOptions.searchCommandOptions.study));
        query.putIfNotEmpty(FamilyDBAdaptor.QueryParams.NAME.key(), familyCommandOptions.searchCommandOptions.name);
        query.putIfNotEmpty(FamilyDBAdaptor.QueryParams.FATHER.key(), familyCommandOptions.searchCommandOptions.father);
        query.putIfNotEmpty(FamilyDBAdaptor.QueryParams.MOTHER.key(), familyCommandOptions.searchCommandOptions.mother);
        query.putIfNotEmpty(FamilyDBAdaptor.QueryParams.MEMBER.key(), familyCommandOptions.searchCommandOptions.member);
        query.putIfNotEmpty(FamilyDBAdaptor.QueryParams.ANNOTATION.key(), familyCommandOptions.searchCommandOptions.annotation);
        query.putIfNotNull(FamilyDBAdaptor.QueryParams.MEMBERS_PARENTAL_CONSANGUINITY.key(),
                familyCommandOptions.searchCommandOptions.parentalConsanguinity);

        if (familyCommandOptions.searchCommandOptions.numericOptions.count) {
            return openCGAClient.getFamilyClient().count(query);
        } else {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.putIfNotEmpty(QueryOptions.INCLUDE, familyCommandOptions.searchCommandOptions.dataModelOptions.include);
            queryOptions.putIfNotEmpty(QueryOptions.EXCLUDE, familyCommandOptions.searchCommandOptions.dataModelOptions.exclude);
            queryOptions.put(QueryOptions.SKIP, familyCommandOptions.searchCommandOptions.numericOptions.skip);
            queryOptions.put(QueryOptions.LIMIT, familyCommandOptions.searchCommandOptions.numericOptions.limit);

            return openCGAClient.getFamilyClient().search(query, queryOptions);
        }
    }

//
//    private QueryResponse<Family> update() throws CatalogException, IOException {
//        logger.debug("Updating individual information");
//
//        ObjectMap params = new ObjectMap();
//        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.NAME.key(), familyCommandOptions.updateCommandOptions.name);
//        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.FATHER_ID.key(), familyCommandOptions.updateCommandOptions.fatherId);
//        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.MOTHER_ID.key(), familyCommandOptions.updateCommandOptions.motherId);
//        if (StringUtils.isNotEmpty(familyCommandOptions.updateCommandOptions.children)) {
//            List<String> childIds = Arrays.asList(StringUtils.split(familyCommandOptions.updateCommandOptions.children, ","));
//            params.put(FamilyDBAdaptor.QueryParams.MEMBER_ID.key(), childIds);
//        }
//        params.putIfNotEmpty(FamilyDBAdaptor.QueryParams.DESCRIPTION.key(), familyCommandOptions.updateCommandOptions.description);
//        params.putIfNotNull(FamilyDBAdaptor.QueryParams.MEMBERS_PARENTAL_CONSANGUINITY.key(),
//                familyCommandOptions.updateCommandOptions.parentalConsanguinity);
//
//        return openCGAClient.getFamilyClient().update(familyCommandOptions.updateCommandOptions.family,
//                resolveStudy(familyCommandOptions.updateCommandOptions.study), params);
//    }
//
    private QueryResponse<FamilyAclEntry> updateAcl() throws IOException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = familyCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotNull("permissions", commandOptions.permissions);
        bodyParams.putIfNotNull("action", commandOptions.action);
        bodyParams.putIfNotNull("family", commandOptions.id);

        return openCGAClient.getFamilyClient().updateAcl(commandOptions.memberId, queryParams, bodyParams);
    }


}
