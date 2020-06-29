package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataReader;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PendingVariantsReader extends AbstractHBaseDataReader<Variant> {

    private final PendingVariantsDescriptor descriptor;

    public PendingVariantsReader(Query query, PendingVariantsDescriptor descriptor, VariantHadoopDBAdaptor dbAdaptor) {
        this(query, descriptor, dbAdaptor.getHBaseManager(), dbAdaptor.getTableNameGenerator());
    }

    public PendingVariantsReader(Query query, PendingVariantsDescriptor descriptor,
                                 HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator) {
        super(hBaseManager, descriptor.getTableName(tableNameGenerator), parseScan(query));
        this.descriptor = descriptor;
    }

    @Override
    public boolean pre() {
        try {
            descriptor.createTableIfNeeded(tableName, hBaseManager);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    protected List<Variant> convert(List<Result> results) {
        List<Variant> variants = new ArrayList<>(results.size());
        for (Result result : results) {
            variants.add(VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(result.getRow()));
        }
        return variants;
    }

    private static List<Scan> parseScan(Query query) {
        List<Scan> scans = new ArrayList<>();
        List<Region> regions = null;
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.REGION)) {
            regions = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));
        }
        if (regions == null) {
            scans.add(new Scan());
        } else {
            for (Region region : regions) {
                Scan scan = new Scan();
                VariantHBaseQueryParser.addRegionFilter(scan, region);
                scans.add(scan);
            }
        }
        return scans;
    }


}
