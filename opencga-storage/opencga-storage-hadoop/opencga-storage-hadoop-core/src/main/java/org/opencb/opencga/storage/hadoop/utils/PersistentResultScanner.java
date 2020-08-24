package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Persistent ResultScanner that will resume the scanner in event of UnknownScannerException.
 *
 * Created by jacobo on 05/01/19.
 */
public class PersistentResultScanner extends AbstractClientScanner {

    private final HBaseManager hBaseManager;
    private final Scan scan;
    private final String tableName;
    protected ResultScanner scanner; // test purposes
    private int scanners = 0;
    private byte[] lastRow = null;
    private boolean empty = false;
    private static Logger logger = LoggerFactory.getLogger(PersistentResultScanner.class);

    PersistentResultScanner(HBaseManager hBaseManager, Scan scan, String tableName) throws IOException {
        this.scanner = null;
        this.hBaseManager = hBaseManager;
        this.scan = scan == null ? new Scan() : new Scan(scan); // Copy scan, as it can me modified.
        this.tableName = tableName;
        checkValid(this.scan);
        initScanMetrics(this.scan);
        obtainNewScanner();
    }

    private void checkValid(Scan scan) {
        String msg = getInvalidScanMessage(scan);
        if (msg != null) {
            throw new IllegalArgumentException(msg);
        }
    }

    static boolean isValid(Scan scan) {
        return getInvalidScanMessage(scan) == null;
    }

    static String getInvalidScanMessage(Scan scan) {
        if (scan == null) {
            return null;
        }
        if (scan.isReversed()) {
            return "Can not use a " + PersistentResultScanner.class.getName() + " with reversed results";
        }
        if (scan.getAllowPartialResults()) {
            return "Can not use a " + PersistentResultScanner.class.getName() + " with partial results";
        }
        return null;
    }

    @Override
    public Result next() throws IOException {
        return next(true);
    }

    private Result next(boolean retry) throws IOException {
        try {
            Result result = scanner.next();
            if (result == null) {
                lastRow = null;
                empty = true;
            } else {
                lastRow = result.getRow();
            }
            return result;
        } catch (UnknownScannerException e) {
            if (empty) {
                return null;
            }
            if (retry) {
                logger.info("Renew lost HBase scanner: {}", e.getMessage());
                logger.debug("Ignore HBase UnknownScannerException", e);
                // Obtain new scanner
                obtainNewScanner();
                return next(false);
            } else {
                throw e;
            }
        }
    }

    private void obtainNewScanner() throws IOException {
        if (lastRow != null) {
            scan.setStartRow(calculateTheClosestNextRowKeyForPrefix());
        }
        scanner = hBaseManager.act(tableName, table -> {
            return table.getScanner(scan);
        });
        scanners++;
        if (scanner instanceof AbstractClientScanner) {
            mergeScanMetrics();
        }
    }

    private byte[] calculateTheClosestNextRowKeyForPrefix() {
        return new Scan().setRowPrefixFilter(lastRow).getStopRow();
    }

    private void mergeScanMetrics() {
        if (scan.isScanMetricsEnabled()) {
            ScanMetrics prevScanMetrics = null;
            if (scanMetrics != null) {
                prevScanMetrics = this.scanMetrics;
            }
            scanMetrics = ((AbstractClientScanner) scanner).getScanMetrics();
            if (prevScanMetrics != null) {
                Map<String, Long> metricsMap = scanMetrics.getMetricsMap();
                for (Map.Entry<String, Long> entry : prevScanMetrics.getMetricsMap().entrySet()) {
                    scanMetrics.setCounter(entry.getKey(), entry.getValue() + metricsMap.getOrDefault(entry.getKey(), 0L));
                }
            }
        }
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public boolean renewLease() {
        if (scanner instanceof AbstractClientScanner) {
            return ((AbstractClientScanner) scanner).renewLease();
        } else {
            return false;
        }
    }

    public int getScannersCount() {
        return scanners;
    }
}
