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
     *  - '@{groupId}' referring to a {@link Group}.
     *  - '{userId}' referring to a specific user.
     * @param dbAdaptorFactory dbAdaptorFactory
     * @param studyId studyId
     * @param member member
     * @throws CatalogDBException CatalogDBException
     */
    public static void checkMember(DBAdaptorFactory dbAdaptorFactory, long studyId, String member)
            throws CatalogDBException {
        if (member.equals("*")) {
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
