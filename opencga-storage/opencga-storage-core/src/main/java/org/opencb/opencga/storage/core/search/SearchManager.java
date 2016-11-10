package org.opencb.opencga.storage.core.search;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.config.SearchConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 09/11/16.
 */
public class SearchManager {

    private SearchConfiguration searchConfiguration;
    private static VariantToSolrConverter variantToSolrConverter;
    private static HttpSolrClient solrServer;

    public SearchManager() {
    }

    public SearchManager(SearchConfiguration searchConfiguration) {
        this.searchConfiguration = searchConfiguration;

        if (searchConfiguration.getHost() != null && searchConfiguration.getCollection() != null && solrServer == null) {
            solrServer = new HttpSolrClient(searchConfiguration.getHost() + "/" + searchConfiguration.getCollection());
            HttpClientUtil.setBasicAuth((DefaultHttpClient) solrServer.getHttpClient(), searchConfiguration.getUser(),
                    searchConfiguration.getPassword());
        }

        if (variantToSolrConverter == null) {
            variantToSolrConverter = new VariantToSolrConverter();
        }
    }

    public void writeVariantsToSolr(List<Variant> variants) {

        List<VariantSolr> variantSolrs = convertVariantsToSolrBeans(variants);

        if (!variantSolrs.isEmpty()) {
            try {
                UpdateResponse updateResponse = solrServer.addBeans(variantSolrs);
                if (0 == updateResponse.getStatus()) {
                    solrServer.commit();
                }
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private List<VariantSolr> convertVariantsToSolrBeans(List<Variant> variants) {
        List<VariantSolr> variantSolrs = new ArrayList<VariantSolr>();
        for (Variant variant : variants) {
            variantSolrs.add(variantToSolrConverter.convertToStorageType(variant));
        }
        return variantSolrs;
    }
}
