package org.opencb.opencga.app.cli.main.parser;

import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

public interface ParamParser {

    void parseParams(String[] args) throws CatalogAuthenticationException;


}
