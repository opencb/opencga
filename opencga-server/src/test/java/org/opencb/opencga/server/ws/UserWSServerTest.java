package org.opencb.opencga.server.ws;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;

public class UserWSServerTest {

    //
//http://localhost:8080/opencga/rest/test/echo/hola


    @Test
    public void testCreateUser(){
        //http://localhost:8080/opencga/rest/users/create?id=ralonso&name=ralonso&email=ralonso%40cipf.es&organization=cipf&role=none&password=ralonso&status=none

        //http://localhost:8080/opencga/rest/projects/create?userId=ralonso&sessionId=NTZb8IUKtKSyci4jj5gN&name=v&alias=v&description=v&organization=v

        Client client = Client.create();

        WebResource webResource = client.resource("http://localhost:8080/opencga/rest/users/create");

        MultivaluedMap queryParams = new MultivaluedMapImpl();
        queryParams.add("id", "ralonso");
        queryParams.add("name", "ralonso");
        queryParams.add("email", "ralonso@cipf.es");
        queryParams.add("organization", "cipf");
        queryParams.add("role", "none");
        queryParams.add("password", "ralonso");
        queryParams.add("status", "none");
        String s = webResource.queryParams(queryParams).get(String.class);

        System.out.println("s = " + s);
    }




}
