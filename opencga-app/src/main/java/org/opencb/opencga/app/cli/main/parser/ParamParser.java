package org.opencb.opencga.app.cli.main.parser;

import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;

public interface ParamParser {

    String[] parseParams(String[] args) throws CatalogAuthenticationException;


}
