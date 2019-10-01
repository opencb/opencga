package org.opencb.opencga.client.exceptions;

import org.opencb.commons.datastore.core.result.Error;

public class ClientException extends Exception {

    public ClientException(String message) {
        super(message);
    }

    public ClientException(Error error) {
        super(error.getDescription());
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }

}
