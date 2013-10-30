package org.opencb.opencga.server;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Created with IntelliJ IDEA.
 * User: fsalavert
 * Date: 10/28/13
 * Time: 12:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class OpenCGAServer extends ResourceConfig {
    public OpenCGAServer() {
        packages("org.opencb.opencga.server.ws").register(MultiPartFeature.class);
    }
}
