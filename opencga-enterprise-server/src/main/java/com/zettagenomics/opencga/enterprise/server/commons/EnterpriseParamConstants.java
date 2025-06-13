package com.zettagenomics.opencga.enterprise.server.commons;

import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParser.*;

public class EnterpriseParamConstants {


    public static final String CVDB_INDEX_TASK_PARAMS_DESCRIPTION = "Parameters: " + CvdbIndexTask.DESCRIPTION;
    public static final String META_TOKEN_DESCRIPTION = "Token to log in";
    public static final String META_ENVIRONMENT_DESCRIPTION = "Environment of the app";
    public static final String META_HOST_DESCRIPTION = "Opencga host without environment";
    public static final String API_CATEGORY_DESCRIPTION = "List of categories to get API from";
    public static final String SSO_LOGIN_CALLBACK_DESCRIPTION = "Callback URL";
    public static final String SSO_LOGOUT_CALLBACK_DESCRIPTION = "Callback URL";
    public static final String SSO_LOGOUT_SUCCESS_DESCRIPTION = "Successfully logout from CAS service";
    public static final String APPROXIMATE_COUNT_DESCRIPTION = "Get an approximate count, instead of an exact total count. Reduces execution time";
    public static final String APPROXIMATE_COUNT_SAMPLING_SIZE_DESCRIPTION = "Sampling size to get the approximate count. Larger values increase accuracy but also increase execution time";
    public static final String CLINICAL_INTERPRETATION_AGGREGATION_STATS_FIELD_DESCRIPTION = "List of facet fields separated by semicolons, e.g.: "
            + "panelIds;methodName. For nested faceted fields use >>, e.g.: panelIds>>methodName. Accepted values: "
            + CI_FACET_FIELDS;
    public static final String CLINICAL_AGGREGATION_STATS_FIELD_DESCRIPTION = "List of facet fields separated by semicolons, e.g.: type;disorderId"
            + ". For nested faceted fields use >>, e.g.: type>>disorderId. Accepted values: "
            + CA_FACET_FIELDS;
    public static final String CLINICAL_VARIANT_AGGREGATION_STATS_FIELD_DESCRIPTION = "List of facet fields separated by semicolons, e.g.: "
            + "geneName;tier. For nested faceted fields use >>, e.g.: geneName>>tier. Accepted values: "
            + CVE_FACET_FIELDS;
    public static final String CLINICAL_VARIANT_VARIANT_ID_DESCRIPTION = "Variant ID (or comma separated list of variant IDs)";

    public static final String FEDERATION_CREATE_DESCRIPTION = "JSON containing the new Federation object";
    public static final String FEDERATION_RESET_DESCRIPTION = "Federation server id to reset";
    public static final String FEDERATION_UPDATE_SERVER_DESCRIPTION = "JSON containing the Federation server parameters to be updated";
    public static final String FEDERATION_SERVER_ID_DESCRIPTION = "Federation server id";
    public static final String FEDERATION_CONNECT_DESCRIPTION = "JSON containing the Federation server configuration";
    public static final String FEDERATION_CLIENT_ID_SYNC = "Federation client id to be synchronized";
    public static final String FEDERATION_UPDATE_CLIENT_DESCRIPTION = "JSON containing the Federation client parameters to be updated";
    public static final String FEDERATION_SHARE_ACTION_DESCRIPTION = "Action to be performed: ADD access or REMOVE access.";
    public static final String FEDERATION_SHARE_USERS_DESCRIPTION = "JSON containing the list of users to which this action will be applied.";
    public static final String FEDERATION_CLIENT_ID_DESCRIPTION = "Federation client id";
    public static final String FEDERATION_REDIRECT_URL_DESCRIPTION = "Original URL";
    public static final String FEDERATION_REDIRECT_BODY_DESCRIPTION = "JSON containing the POST object";
    public static final String USERS_CALLBACK_URL_DESCRIPTION = "Callback URL";
    public static final String USERS_LOGOUT_DESCRIPTION = "Successfully logout from CAS service";


}
