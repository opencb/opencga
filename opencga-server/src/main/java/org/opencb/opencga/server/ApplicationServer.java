package org.opencb.opencga.server;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;

/**
 * Created with IntelliJ IDEA.
 * User: fsalavert
 * Date: 10/28/13
 * Time: 12:14 PM
 * To change this template use File | Settings | File Templates.
 */

@ApplicationPath("/")
public class ApplicationServer extends ResourceConfig {
    public ApplicationServer() {
        super(
                AccountWSServer.class,
                AdminWSServer.class,
                AnalysisWSServer.class,
                BamWSServer.class,
                GeocodingAddressService.class,
                GffWSServer.class,
                JobAnalysisWSServer.class,
                StorageWSServer.class,
                UtilsWSServer.class,
                VcfWSServer.class,
                WSResponse.class,

                MultiPartFeature.class
        );
    }
}
