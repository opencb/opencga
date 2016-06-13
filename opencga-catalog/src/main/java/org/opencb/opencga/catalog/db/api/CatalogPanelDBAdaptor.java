package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.acls.DiseasePanelAcl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * Created by pfurio on 01/06/16.
 */
public interface CatalogPanelDBAdaptor extends CatalogDBAdaptor<DiseasePanel> {

    enum QueryParams implements QueryParam {
        ID("id", INTEGER, ""),
        NAME("name", TEXT, ""),
        DISEASE("disease", TEXT, ""),
        DESCRIPTION("description", TEXT, ""),

        GENES("genes", TEXT_ARRAY, ""),
        REGIONS("regions", TEXT_ARRAY, ""),
        VARIANTS("variants", TEXT_ARRAY, ""),

        STATUS_STATUS("status.status", TEXT, ""),
        STATUS_MSG("status.msg", TEXT, ""),
        STATUS_DATE("status.date", TEXT, ""),

        ACLS("acls", TEXT_ARRAY, ""),
        ACLS_USERS("acls.users", TEXT_ARRAY, ""),
        ACLS_PERMISSIONS("acls.permissions", TEXT_ARRAY, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, "");

        private static Map<String, QueryParams> map = new HashMap<>();
        static {
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

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    default boolean panelExists(long panelId) throws CatalogDBException {
        return count(new Query(QueryParams.ID.key(), panelId)).first() > 0;
    }

    default void checkPanelId(long panelId) throws CatalogDBException {
        if (panelId < 0) {
            throw CatalogDBException.newInstance("Panel id '{}' is not valid: ", panelId);
        }

        if (!panelExists(panelId)) {
            throw CatalogDBException.newInstance("Panel id '{}' does not exist", panelId);
        }
    }

    QueryResult<DiseasePanel> createPanel(long studyId, DiseasePanel diseasePanel, QueryOptions options) throws CatalogDBException;

    QueryResult<DiseasePanel> getPanel(long diseasePanelId, QueryOptions options) throws CatalogDBException;

    default QueryResult<DiseasePanelAcl> getPanelAcl(long panelId, String member) throws CatalogDBException {
        return getPanelAcl(panelId, Arrays.asList(member));
    }

    QueryResult<DiseasePanelAcl> getPanelAcl(long panelId, List<String> members) throws CatalogDBException;

    QueryResult<DiseasePanelAcl> setPanelAcl(long panelId, DiseasePanelAcl acl, boolean override) throws CatalogDBException;

    void unsetPanelAcl(long panelId, List<String> members, List<String> permissions) throws CatalogDBException;

    void unsetPanelAclsInStudy(long studyId, List<String> members) throws CatalogDBException;

    long getStudyIdByPanelId(long panelId) throws CatalogDBException;

}
