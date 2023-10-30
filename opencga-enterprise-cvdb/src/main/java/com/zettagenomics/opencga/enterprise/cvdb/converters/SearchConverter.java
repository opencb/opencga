package com.zettagenomics.opencga.enterprise.cvdb.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;

public class SearchConverter<M, N> {

    protected ObjectMapper mapper;
    protected ObjectReader mapReader;

    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    public static SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    protected static Logger logger = LoggerFactory.getLogger(SearchConverter.class);

    public SearchConverter() {
        this.mapper = JacksonUtils.getDefaultObjectMapper();
        this.mapReader = mapper.readerFor(HashMap.class);
    }

    public M toModel(N input) throws CvdbException {
        return null;
    }
}
