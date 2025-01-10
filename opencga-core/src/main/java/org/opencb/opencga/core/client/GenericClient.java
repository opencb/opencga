package org.opencb.opencga.core.client;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.LoginParams;
import org.opencb.opencga.core.response.RestResponse;

import java.util.Map;

public class GenericClient extends ParentClient {

    public GenericClient(String token, ClientConfiguration clientConfiguration) {
        super(token, clientConfiguration);
    }

    /**
     * Get an anonymous token to gain access to the system.
     * @param organization Organization id.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<AuthenticationResponse> anonymous(String organization) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("organization", organization);
        return execute("users", null, null, null, "anonymous", params, POST, AuthenticationResponse.class);
    }

    /**
     * Get identified and gain access to the system.
     * @param data JSON containing the authentication parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<AuthenticationResponse> login(LoginParams data) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.put("body", data);
        return execute("users", null, null, null, "login", params, POST, AuthenticationResponse.class);
    }

    @Override
    public <T> RestResponse<T> execute(String category, String action, Map<String, Object> params, String method, Class<T> clazz)
            throws ClientException {
        return super.execute(category, action, params, method, clazz);
    }

    public <T> RestResponse<T> execute(String category, String action, Map<String, Object> params, Object body, String method,
                                       Class<T> clazz) throws ClientException {
        if (body != null) {
            params = params == null ? new ObjectMap() : params;
            params.put("body", body);
        }
        return super.execute(category, action, params, method, clazz);
    }

    @Override
    public <T> RestResponse<T> execute(String category, String id, String action, Map<String, Object> params, String method, Class<T> clazz)
            throws ClientException {
        return super.execute(category, id, action, params, method, clazz);
    }

    public <T> RestResponse<T> execute(String category, String id, String action, Map<String, Object> params, Object body, String method,
                                       Class<T> clazz) throws ClientException {
        if (body != null) {
            params = params == null ? new ObjectMap() : params;
            params.put("body", body);
        }
        return super.execute(category, id, action, params, method, clazz);
    }

    @Override
    public <T> RestResponse<T> execute(String category1, String id1, String category2, String id2, String action,
                                       Map<String, Object> params, String method, Class<T> clazz) throws ClientException {
        return super.execute(category1, id1, category2, id2, action, params, method, clazz);
    }

    public <T> RestResponse<T> execute(String category1, String id1, String category2, String id2, String action,
                                       Map<String, Object> params, Object body, String method, Class<T> clazz) throws ClientException {
        if (body != null) {
            params = params == null ? new ObjectMap() : params;
            params.put("body", body);
        }
        return super.execute(category1, id1, category2, id2, action, params, method, clazz);
    }
}
