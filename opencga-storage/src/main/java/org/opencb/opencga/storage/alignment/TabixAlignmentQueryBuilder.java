package org.opencb.opencga.storage.alignment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.commons.bioformats.alignment.Alignment;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.CellbaseCredentials;
import org.opencb.opencga.lib.auth.SqliteCredentials;
import org.opencb.opencga.lib.auth.TabixCredentials;
import org.opencb.opencga.lib.common.XObject;
import org.opencb.opencga.storage.indices.SqliteManager;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class TabixAlignmentQueryBuilder implements AlignmentQueryBuilder {

    private TabixCredentials tabixCredentials;
    private CellbaseCredentials cellbaseCredentials;
    
    private SqliteCredentials sqliteCredentials;
    private SqliteManager sqliteManager;

    public TabixAlignmentQueryBuilder(SqliteCredentials sqliteCredentials, 
                                         TabixCredentials tabixCredentials, 
                                         CellbaseCredentials cellbaseCredentials) {
        this.sqliteCredentials = sqliteCredentials;
        this.tabixCredentials = tabixCredentials;
        this.cellbaseCredentials = cellbaseCredentials;
        this.sqliteManager = new SqliteManager();
    }
    
    @Override
    public QueryResult<Alignment> getAlignmentsByRegion(String chromosome, long start, long end, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult<Alignment> getAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public QueryResult<List<ObjectMap>> getAlignmentsHistogramByRegion(
            String chromosome, long start, long end, boolean histogramLogarithm, int histogramMax) {
        QueryResult<List<ObjectMap>> queryResult = new QueryResult<>(String.format("%s:%d-%d", chromosome, start, end)); // TODO Fill metadata
        List<ObjectMap> data = new ArrayList<>();
        
        long startTime = System.currentTimeMillis();
        
        Path metaDir = getMetaDir(sqliteCredentials.getPath());
        String fileName = sqliteCredentials.getPath().getFileName().toString();

        try {
            long startDbTime = System.currentTimeMillis(); 
            sqliteManager.connect(metaDir.resolve(Paths.get(fileName)), true);
            System.out.println("SQLite path: " + metaDir.resolve(Paths.get(fileName)).toString());
            String queryString = "SELECT * FROM chunk WHERE chromosome='" + chromosome + "' AND start <= " + end + " AND end >= " + start;
            List<XObject> queryResults = sqliteManager.query(queryString);
            sqliteManager.disconnect(true);
            queryResult.setDbTime(System.currentTimeMillis() - startDbTime);
            
            int resultSize = queryResults.size();

            if (resultSize > histogramMax) { // Need to group results to fit maximum size of the histogram
                int sumChunkSize = resultSize / histogramMax;
                int i = 0, j = 0;
                int featuresCount = 0;
                ObjectMap item = null;
                
                for (XObject result : queryResults) {
                    featuresCount += result.getInt("features_count");
                    if (i == 0) {
                        item = new ObjectMap("chromosome", result.getString("chromosome"));
                        item.put("chunk_id", result.getInt("chunk_id"));
                        item.put("start", result.getInt("start"));
                    } else if (i == sumChunkSize - 1 || j == resultSize - 1) {
                        if (histogramLogarithm) {
                            item.put("features_count", (featuresCount > 0) ? Math.log(featuresCount) : 0);
                        } else {
                            item.put("features_count", featuresCount);
                        }
                        item.put("end", result.getInt("end"));
                        data.add(item);
                        i = -1;
                        featuresCount = 0;
                    }
                    j++;
                    i++;
                }
            } else {
                for (XObject result : queryResults) {
                    ObjectMap item = new ObjectMap("chromosome", result.getString("chromosome"));
                    item.put("chunk_id", result.getInt("chunk_id"));
                    item.put("start", result.getInt("start"));
                    if (histogramLogarithm) {
                        int features_count = result.getInt("features_count");
                        result.put("features_count", (features_count > 0) ? Math.log(features_count) : 0);
                    } else {
                        item.put("features_count", result.getInt("features_count"));
                    }
                    item.put("end", result.getInt("end"));
                    data.add(item);
                }
            }
        } catch (ClassNotFoundException | SQLException ex ) {
            Logger.getLogger(TabixAlignmentQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }
        
        queryResult.setResult(data);
        queryResult.setNumResults(data.size());
        queryResult.setTime(System.currentTimeMillis() - startTime);
        
        return queryResult;
    }
    
    private Path getMetaDir(Path file) {
        String inputName = file.getFileName().toString();
        return file.getParent().resolve(".meta_" + inputName);
    }

}
