package org.opencb.opencga.catalog.utils;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Group;

import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 16/06/16.
 */
public class CatalogMemberValidator {

    /**
     * Checks if the list of members are all valid.
     *
     * The "members" can be:
     *  - '*' referring to all the users.
     *  - 'anonymous' referring to the anonymous user.
     *  - '@{groupId}' referring to a {@link Group}.
     *  - '{userId}' referring to a specific user.
     * @param dbAdaptorFactory dbAdaptorFactory
     * @param studyId studyId
     * @param members List of members
     * @throws CatalogDBException CatalogDBException
     */
    public static void checkMembers(DBAdaptorFactory dbAdaptorFactory, long studyId, List<String> members)
            throws CatalogDBException {
        for (String member : members) {
            checkMember(dbAdaptorFactory, studyId, member);
        }
    }

    /**
     * Checks if the member is valid.
     *
     * The "member" can be:
     *  - '*' referring to all the users.
     *  - 'anonymous' referring to the anonymous user.
     *  - '@{groupId}' referring to a {@link Group}.
     *  - '{userId}' referring to a specific user.
     * @param dbAdaptorFactory dbAdaptorFactory
     * @param studyId studyId
     * @param member member
     * @throws CatalogDBException CatalogDBException
     */
    public static void checkMember(DBAdaptorFactory dbAdaptorFactory, long studyId, String member)
            throws CatalogDBException {
        if (member.equals("*") || member.equals("anonymous")) {
            return;
        } else if (member.startsWith("@")) {
            QueryResult<Group> queryResult = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member,
                    Collections.emptyList());
            if (queryResult.getNumResults() == 0) {
                throw CatalogDBException.idNotFound("Group", member);
            }
        } else {
            dbAdaptorFactory.getCatalogUserDBAdaptor().checkId(member);
        }
    }

}
