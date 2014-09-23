package org.opencb.opencga.server.ws;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;

public class ProjectWSServerTest {

    //
//http://localhost:8080/opencga/rest/test/echo/hola


    @Test
    public void testCreateProject(){
        //http://localhost:8080/opencga/rest/projects/create?userId=ralonso&sessionId=NTZb8IUKtKSyci4jj5gN&name=v&alias=v&description=v&organization=v

        Client client = Client.create();

        WebResource webResource = client.resource("http://localhost:8080/opencga/rest/projects/create");

        MultivaluedMap queryParams = new MultivaluedMapImpl();
        queryParams.add("userId", "ralonso");
        queryParams.add("sessionId", "uCLzBJL6QOqUX32DHxGV");
        queryParams.add("name", "prName");
        queryParams.add("alias", "prAlias");
        queryParams.add("description", "prDescr");
        queryParams.add("organization", "prOrg");
        String s = webResource.queryParams(queryParams).get(String.class);

        System.out.println("s = " + s);
    }


}
