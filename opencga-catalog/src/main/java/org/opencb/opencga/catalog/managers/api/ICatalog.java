package org.opencb.opencga.catalog.managers.api;

import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created by jacobo on 10/02/15.
 */
public interface ICatalog {

    String getSessionId();

    void setSessionId(String sessionId);

    String getUserId();

    void setUserId(String userId);

    IUserManager users();

    IProjectManager projects();

    IStudyManager studies();

    IFileManager files();

    IJobManager jobs();

    void close() throws CatalogException;

}
