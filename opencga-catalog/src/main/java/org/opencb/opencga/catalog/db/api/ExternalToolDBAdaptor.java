package org.opencb.opencga.catalog.db.api;

import org.apache.commons.collections4.map.LinkedMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.externalTool.ExternalTool;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface ExternalToolDBAdaptor extends CoreDBAdaptor<ExternalTool> {

    OpenCGAResult<ExternalTool> insert(long studyUid, ExternalTool externalTool, QueryOptions options)
            throws CatalogException;

    enum QueryParams implements QueryParam {
        ID("id", TEXT, ""),
        UID("uid", LONG, ""),
        UUID("uuid", TEXT, ""),
        NAME("name", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),
        DRAFT("draft", BOOLEAN, ""),
        TYPE("type", TEXT, ""),
        SCOPE("scope", TEXT, ""),
        TAGS("tags", TEXT_ARRAY, ""),
        WORKFLOW("workflow", OBJECT, ""),
        WORKFLOW_MANAGER("workflow.manager", OBJECT, ""),
        WORKFLOW_REPOSITORY("workflow.repository", OBJECT, ""),
        WORKFLOW_REPOSITORY_NAME("workflow.repository.name", TEXT, ""),
        WORKFLOW_SCRIPTS("workflow.scripts", OBJECT, ""),
        CONTAINER("container", OBJECT, ""),
        CONTAINER_NAME("container.name", TEXT, ""),
        VARIABLES("variables", OBJECT, ""),
        MINIMUM_REQUIREMENTS("minimumRequirements", OBJECT, ""),
        INTERNAL_REGISTRATION_USER_ID("internal.registrationUserId", TEXT, ""),
        RELEASE("release", INTEGER, ""), //  Release where the sample was created
        SNAPSHOT("snapshot", INTEGER, ""), // Last version of sample at release = snapshot
        VERSION("version", INTEGER, ""), // Version of the sample
        CREATION_DATE("creationDate", DATE, ""),
        MODIFICATION_DATE("modificationDate", DATE, ""),
        ATTRIBUTES("attributes", OBJECT, ""),
        STUDY_UID("studyUid", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""); // Alias to studyId in the database. Only for the webservices.

        private static Map<String, QueryParams> map;

        static {
            map = new LinkedMap();
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }
    }

}
