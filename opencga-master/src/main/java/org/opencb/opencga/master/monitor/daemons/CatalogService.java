package org.opencb.opencga.master.monitor.daemons;

import org.opencb.opencga.catalog.managers.CatalogManager;

import java.io.Closeable;
import java.io.IOException;

public class CatalogService extends MonitorParentDaemon implements Closeable {


    public CatalogService(int interval, String token, CatalogManager catalogManager) {
        super(interval, token, catalogManager);
    }

    @Override
    public void apply() throws Exception {

    }

    private void checkCVDB() {
    }

    @Override
    public void close() throws IOException {

    }
}
