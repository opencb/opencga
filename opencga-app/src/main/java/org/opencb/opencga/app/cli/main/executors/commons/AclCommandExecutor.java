package org.opencb.opencga.app.cli.main.executors.commons;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.client.rest.StudyClient;

import java.io.IOException;

/**
 * Created by pfurio on 27/07/16.
 */
public class AclCommandExecutor<T,U> {

    public QueryResponse<U> acls(AclCommandOptions.AclsCommandOptions aclCommandOptions, AbstractParentClient<T,U> client)
            throws CatalogException,IOException {
        return client.getAcls(aclCommandOptions.id);
    }

    public QueryResponse<U> aclsCreate(AclCommandOptions.AclsCreateCommandOptions aclCommandOptions, AbstractParentClient<T,U> client)
            throws CatalogException,IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull("permissions", aclCommandOptions.permissions);
        return client.createAcl(aclCommandOptions.id, aclCommandOptions.members, objectMap);
    }

    public QueryResponse<StudyAclEntry> aclsCreateTemplate(AclCommandOptions.AclsCreateCommandOptionsTemplate aclCommandOptions,
                                                           StudyClient client) throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull("permissions", aclCommandOptions.permissions);
        objectMap.putIfNotNull("templateId", aclCommandOptions.templateId);
        return client.createAcl(aclCommandOptions.id, aclCommandOptions.members, objectMap);
    }

    public QueryResponse<U> aclMemberDelete(AclCommandOptions.AclsMemberDeleteCommandOptions aclCommandOptions,
                                            AbstractParentClient<T,U> client) throws CatalogException,IOException {
        return client.deleteAcl(aclCommandOptions.id, aclCommandOptions.memberId);
    }

    public QueryResponse<U> aclMemberInfo(AclCommandOptions.AclsMemberInfoCommandOptions aclCommandOptions,
                                          AbstractParentClient<T,U> client) throws CatalogException,IOException {
        return client.getAcl(aclCommandOptions.id, aclCommandOptions.memberId);
    }

    public QueryResponse<U> aclMemberUpdate(AclCommandOptions.AclsMemberUpdateCommandOptions aclCommandOptions,
                                            AbstractParentClient<T,U> client) throws CatalogException,IOException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull(StudyClient.AclParams.ADD_PERMISSIONS.key(), aclCommandOptions.addPermissions);
        objectMap.putIfNotNull(StudyClient.AclParams.REMOVE_PERMISSIONS.key(), aclCommandOptions.removePermissions);
        objectMap.putIfNotNull(StudyClient.AclParams.SET_PERMISSIONS.key(), aclCommandOptions.setPermissions);
        return client.updateAcl(aclCommandOptions.id, aclCommandOptions.memberId, objectMap);
    }

}
