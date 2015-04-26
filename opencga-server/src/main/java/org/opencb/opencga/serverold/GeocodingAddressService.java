/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.serverold;

import org.opencb.opencga.server.OpenCGAWSServer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

@Path("/geolocation")
public class GeocodingAddressService extends GenericWSServer {

    public GeocodingAddressService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest)
            throws IOException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/test")
    public String getAddress(@DefaultValue("") @QueryParam("latLong") String latLong) {
        String returned = null;
        try {
            URL mapsUrl = new URL("http://maps.googleapis.com/maps/api/geocode/xml?latlng=" + latLong + "&sensor=true");
            InputStream openStream = mapsUrl.openStream();
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(openStream);
            NodeList formattedAddress = doc.getElementsByTagName("formatted_address");
            Element formattedAddressElement = (Element) formattedAddress.item(0);
            returned = formattedAddressElement.getTextContent();
        } catch (Exception e) {
            returned = null;
        }
        return returned;
    }
}
