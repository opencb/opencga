package com.zettagenomics.opencga.enterprise.cva.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.opencga.core.common.JacksonUtils;

import java.util.HashMap;

public class SearchConverter {

    protected ObjectMapper mapper;
    protected ObjectReader mapReader;

    public SearchConverter() {
        this.mapper = JacksonUtils.getDefaultObjectMapper();
        this.mapReader = mapper.readerFor(HashMap.class);
    }
}
