package org.opencb.opencga.catalog.authorization;

import java.security.acl.Permission;

/**
 * Created on 21/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public enum StudyPermission implements Permission {
    LAUNCH_JOBS,
    MANAGE_SAMPLES,
    MANAGE_STUDY,
    READ_STUDY
}
