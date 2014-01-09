package org.opencb.opencga.storage.variant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.lang.StringUtils;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantControl;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.SqliteCredentials;
import org.opencb.opencga.lib.common.XObject;
import org.opencb.opencga.storage.indices.SqliteManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantSqliteQueryBuilder implements VariantQueryBuilder {

    private SqliteCredentials sqliteCredentials;
    private SqliteManager sqliteManager;

    public VariantSqliteQueryBuilder() {
        System.out.println("Variant Query Maker");
    }

    public VariantSqliteQueryBuilder(SqliteCredentials sqliteCredentials) {
        System.out.println("Variant Query Maker");
        this.sqliteCredentials = sqliteCredentials;
        this.sqliteManager = new SqliteManager();
    }

    @Override
    public QueryResult getAllVariantsByRegion(Region region, String studyName, QueryOptions options) {
        Connection con;
        Statement stmt;
        List<VariantInfo> list = new ArrayList<>(100);

        String dbName = (String) options.get("db_name");
        showDb(dbName);
        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            List<String> whereClauses = new ArrayList<>(10);

            StringBuilder regionClauses = new StringBuilder();
            regionClauses.append("( variant_stats.chromosome='").append(region.getChromosome()).append("' AND ");
            regionClauses.append("variant_stats.position>=").append(String.valueOf(region.getStart())).append(" AND ");
            regionClauses.append("variant_stats.position<=").append(String.valueOf(region.getStart())).append(" )");
            regionClauses.append(" ) ");
            whereClauses.add(regionClauses.toString());

            String sql = "SELECT count(*) as count FROM sample ;";

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            int numSamples = 0;

            while (rs.next()) {
                numSamples = rs.getInt("count");
            }

            stmt.close();

            String innerJoinVariantSQL = " left join variant_info on variant.id_variant=variant_info.id_variant ";
            String innerJoinEffectSQL = " inner join variant_effect on variant_effect.chromosome=variant.chromosome AND variant_effect.position=variant.position AND variant_effect.reference_allele=variant.ref AND variant_effect.alternative_allele = variant.alt ";


            sql = "SELECT distinct variant.genes,variant.consequence_types, variant.id_variant, variant_info.key, variant_info.value, sample_info.sample_name, sample_info.allele_1, sample_info.allele_2, variant_stats.chromosome ," +
                    "variant_stats.position , variant_stats.allele_ref , variant_stats.allele_alt , variant_stats.id , variant_stats.maf , variant_stats.mgf, " +
                    "variant_stats.allele_maf , variant_stats.genotype_maf , variant_stats.miss_allele , variant_stats.miss_gt , variant_stats.mendel_err ," +
                    "variant_stats.is_indel , variant_stats.cases_percent_dominant , variant_stats.controls_percent_dominant , variant_stats.cases_percent_recessive , variant_stats.controls_percent_recessive " + //, variant_stats.genotypes  " +
                    " FROM variant_stats " +
                    "inner join variant on variant_stats.chromosome=variant.chromosome AND variant_stats.position=variant.position AND variant_stats.allele_ref=variant.ref AND variant_stats.allele_alt=variant.alt " +
                    //innerJoinEffectSQL +
                    "inner join sample_info on variant.id_variant=sample_info.id_variant " +
                    innerJoinVariantSQL;

            if (whereClauses.size() > 0) {
                StringBuilder where = new StringBuilder(" where ");

                for (int i = 0; i < whereClauses.size(); i++) {
                    where.append(whereClauses.get(i));
                    if (i < whereClauses.size() - 1) {
                        where.append(" AND ");
                    }
                }

                sql += where.toString() + " ORDER BY variant_stats.chromosome , variant_stats.position , variant_stats.allele_ref , variant_stats.allele_alt ;";
            }

            System.out.println(sql);

            System.out.println("Start SQL");
            long start = System.currentTimeMillis();
            stmt = con.createStatement();

            rs = stmt.executeQuery(sql);

            VariantStats vs;
            VariantInfo vi = null;


            String chr = "";
            int pos = 0;
            String ref = "", alt = "";

            System.out.println("End SQL: " + ((System.currentTimeMillis() - start) / 1000.0) + " s.");
            System.out.println("Processing");

            while (rs.next()) {
                if (!rs.getString("chromosome").equals(chr) ||
                        rs.getInt("position") != pos ||
                        !rs.getString("allele_ref").equals(ref) ||
                        !rs.getString("allele_alt").equals(alt)) {


                    chr = rs.getString("chromosome");
                    pos = rs.getInt("position");
                    ref = rs.getString("allele_ref");
                    alt = rs.getString("allele_alt");

//                    if (vi != null && filterGenotypes(vi, numSamples) && filterControls(vi, controlsMAFs)) {
                    if (vi != null) { // Modified by Cristina
                        list.add(vi);
                    }
                    vi = new VariantInfo(chr, pos, ref, alt);
                    vs = new VariantStats(chr, pos, ref, alt,
                            rs.getDouble("maf"), rs.getDouble("mgf"), rs.getString("allele_maf"), rs.getString("genotype_maf"), rs.getInt("miss_allele"),
                            rs.getInt("miss_gt"), rs.getInt("mendel_err"), rs.getInt("is_indel") == 1, rs.getDouble("cases_percent_dominant"), rs.getDouble("controls_percent_dominant"),
                            rs.getDouble("cases_percent_recessive"), rs.getDouble("controls_percent_recessive"));
                    vs.setId(rs.getString("id"));

                    // vi.addGenotypes(rs.getString("genotypes"));

                    vi.addStats(vs);
                    vi.addGenes(rs.getString("genes"));
                    vi.addConsequenceTypes(rs.getString("consequence_types"));
                }

                if (rs.getString("key") != null && rs.getString("value") != null) {

                    vi.addControl(rs.getString("key"), rs.getString("value"));
                }


                String sample = rs.getString("sample_name");
                String gt = rs.getInt("allele_1") + "/" + rs.getInt("allele_2");

                vi.addSammpleGenotype(sample, gt);
                // vi.addGeneAndConsequenceType(rs.getString("gene_name"), rs.getString("consequence_type_obo"));

            }

//            if (vi != null && filterGenotypes(vi, numSamples) && filterControls(vi, controlsMAFs)) {
            if (vi != null) { // Modified by Cristina
                list.add(vi);
            }
            stmt.close();


            System.out.println("Total: (" + list.size() + ")");
            System.out.println("End processing: " + ((System.currentTimeMillis() - start) / 1000.0) + " s.");

            con.close();

        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("STATS: " + e.getClass().getName() + ": " + e.getMessage());
        }

//        return list;
        return new QueryResult();
    }
    
    
    @Override
    public List<QueryResult> getAllVariantsByRegionList(List<Region> region, String studyName, QueryOptions options) {
        return null;  // TODO Implementation needed
    }

    @Override
    public QueryResult<ObjectMap> getVariantsHistogramByRegion(Region region, String studyName, boolean histogramLogarithm, int histogramMax) {
        QueryResult<ObjectMap> queryResult = new QueryResult<>(String.format("%s:%d-%d",
                region.getChromosome(), region.getStart(), region.getEnd())); // TODO Fill metadata
        List<ObjectMap> data = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        Path metaDir = getMetaDir(sqliteCredentials.getPath());
        String fileName = sqliteCredentials.getPath().getFileName().toString();

        try {
            long startDbTime = System.currentTimeMillis();
            sqliteManager.connect(metaDir.resolve(Paths.get(fileName)), true);
            System.out.println("SQLite path: " + metaDir.resolve(Paths.get(fileName)).toString());
            String queryString = "SELECT * FROM chunk WHERE chromosome='" + region.getChromosome() +
                    "' AND start <= " + region.getEnd() + " AND end >= " + region.getStart();
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
                        item.put("chunkId", result.getInt("chunk_id"));
                        item.put("start", result.getInt("start"));
                    } else if (i == sumChunkSize - 1 || j == resultSize - 1) {
                        if (histogramLogarithm) {
                            item.put("featuresCount", (featuresCount > 0) ? Math.log(featuresCount) : 0);
                        } else {
                            item.put("featuresCount", featuresCount);
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
                    item.put("chunkId", result.getInt("chunk_id"));
                    item.put("start", result.getInt("start"));
                    if (histogramLogarithm) {
                        int features_count = result.getInt("features_count");
                        result.put("featuresCount", (features_count > 0) ? Math.log(features_count) : 0);
                    } else {
                        item.put("featuresCount", result.getInt("features_count"));
                    }
                    item.put("end", result.getInt("end"));
                    data.add(item);
                }
            }
        } catch (ClassNotFoundException | SQLException ex ) {
            Logger.getLogger(VariantSqliteQueryBuilder.class.getName()).log(Level.SEVERE, null, ex);
            queryResult.setErrorMsg(ex.getMessage());
        }

        queryResult.setResult(data);
        queryResult.setNumResults(data.size());
        queryResult.setTime(System.currentTimeMillis() - startTime);

        return queryResult;
    }

    @Override
    public QueryResult getStatsByVariant(Variant variant, QueryOptions options) {
        return null;  // TODO Implementation needed
    }

    @Override
    public QueryResult getSimpleStatsByVariant(Variant variant, QueryOptions options) {
        return null;  // TODO Implementation needed
    }

    @Override
    public QueryResult getEffectsByVariant(Variant variant, QueryOptions options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<VariantInfo> getRecords(Map<String, String> options) {
        Connection con;
        Statement stmt;
        List<VariantInfo> list = new ArrayList<>(100);

        String dbName = options.get("db_name");
        showDb(dbName);
        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            List<String> whereClauses = new ArrayList<>(10);

            Map<String, List<String>> sampleGenotypes;
            Map<String, String> controlsMAFs = new LinkedHashMap<>();
            sampleGenotypes = processSamplesGT(options);

            if (options.containsKey("region_list") && !options.get("region_list").equals("")) {

                StringBuilder regionClauses = new StringBuilder("(");
                String[] regions = options.get("region_list").split(",");
                Pattern patternReg = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
                Matcher matcherReg, matcherChr;

                for (int i = 0; i < regions.length; i++) {
                    String region = regions[i];
                    matcherReg = patternReg.matcher(region);
                    if (matcherReg.find()) {
                        String chr = matcherReg.group(1);
                        int start = Integer.valueOf(matcherReg.group(2));
                        int end = Integer.valueOf(matcherReg.group(3));

                        regionClauses.append("( variant_stats.chromosome='").append(chr).append("' AND ");
                        regionClauses.append("variant_stats.position>=").append(start).append(" AND ");
                        regionClauses.append("variant_stats.position<=").append(end).append(" )");

                        if (i < (regions.length - 1)) {
                            regionClauses.append(" OR ");

                        }
                    } else {
                        Pattern patternChr = Pattern.compile("(\\w+)");
                        matcherChr = patternChr.matcher(region);

                        if (matcherChr.find()) {
                            String chr = matcherChr.group();
                            regionClauses.append("( variant_stats.chromosome='").append(chr).append("')");

                            if (i < (regions.length - 1)) {
                                regionClauses.append(" OR ");
                            }
                        } else {
                            System.err.println("ERROR: Region (" + region + ")");
                        }
                    }
                }
                regionClauses.append(" ) ");
                whereClauses.add(regionClauses.toString());
            }

            if (options.containsKey("chr_pos") && !options.get("chr_pos").equals("")) {

                whereClauses.add("variant_stats.chromosome='" + options.get("chr_pos") + "'");
                if (options.containsKey("start_pos") && !options.get("start_pos").equals("")) {
                    whereClauses.add("variant_stats.position>=" + options.get("start_pos"));
                }

                if (options.containsKey("end_pos") && !options.get("end_pos").equals("")) {
                    whereClauses.add("variant_stats.position<=" + options.get("end_pos"));
                }
            }


            if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
                String val = options.get("mend_error");
                String opt = options.get("option_mend_error");
                whereClauses.add("variant_stats.mendel_err " + opt + " " + val);

            }

            if (options.containsKey("is_indel") && options.get("is_indel").equalsIgnoreCase("on")) {
                whereClauses.add("variant_stats.is_indel=1");
            }

            if (options.containsKey("maf") && !options.get("maf").equals("")) {
                String val = options.get("maf");
                String opt = options.get("option_maf");
                whereClauses.add("variant_stats.maf " + opt + " " + val);

            }

            if (options.containsKey("mgf") && !options.get("mgf").equals("")) {
                String val = options.get("mgf");
                String opt = options.get("option_mgf");
                whereClauses.add("variant_stats.mgf " + opt + " " + val);

            }

            if (options.containsKey("miss_allele") && !options.get("miss_allele").equals("")) {
                String val = options.get("miss_allele");
                String opt = options.get("option_miss_allele");
                whereClauses.add("variant_stats.miss_allele " + opt + " " + val);
            }
            if (options.containsKey("miss_gt") && !options.get("miss_gt").equals("")) {
                String val = options.get("miss_gt");
                String opt = options.get("option_miss_gt");
                whereClauses.add("variant_stats.miss_gt " + opt + " " + val);

            }
            if (options.containsKey("cases_percent_dominant") && !options.get("cases_percent_dominant").equals("")) {
                String val = options.get("cases_percent_dominant");
                String opt = options.get("option_cases_dom");
                whereClauses.add("variant_stats.cases_percent_dominant " + opt + " " + val);
            }

            if (options.containsKey("controls_percent_dominant") && !options.get("controls_percent_dominant").equals("")) {
                String val = options.get("controls_percent_dominant");
                String opt = options.get("option_controls_dom");
                whereClauses.add("variant_stats.controls_percent_dominant " + opt + " " + val);
            }

            if (options.containsKey("cases_percent_recessive") && !options.get("cases_percent_recessive").equals("")) {
                String val = options.get("cases_percent_recessive");
                String opt = options.get("option_cases_rec");
                whereClauses.add("variant_stats.cases_percent_recessive " + opt + " " + val);
            }

            if (options.containsKey("controls_percent_recessive") && !options.get("controls_percent_recessive").equals("")) {
                String val = options.get("controls_percent_recessive");
                String opt = options.get("option_controls_rec");
                whereClauses.add("variant_stats.controls_percent_recessive " + opt + " " + val);
            }


            if (options.containsKey("biotype") && !options.get("biotype").equals("")) {
                String[] biotypes = options.get("biotype").split(",");

                StringBuilder biotypesClauses = new StringBuilder(" ( ");

                for (int i = 0; i < biotypes.length; i++) {
                    biotypesClauses.append("variant_effect.feature_biotype LIKE '%").append(biotypes[i]).append("%'");

                    if (i < (biotypes.length - 1)) {
                        biotypesClauses.append(" OR ");
                    }
                }

                biotypesClauses.append(" ) ");
                whereClauses.add(biotypesClauses.toString());
            }

            if (options.containsKey("exc_1000g_controls") && options.get("exc_1000g_controls").equalsIgnoreCase("on")) {
                whereClauses.add("(key NOT LIKE '1000G%' OR key is null)");
            } else if (options.containsKey("maf_1000g_controls") && !options.get("maf_1000g_controls").equals("")) {
                controlsMAFs.put("1000G", options.get("maf_1000g_controls"));
            }


            if (options.containsKey("exc_bier_controls") && options.get("exc_bier_controls").equalsIgnoreCase("on")) {
                whereClauses.add("(key NOT LIKE 'BIER%' OR key is null)");
            } else if (options.containsKey("maf_bier_controls") && !options.get("maf_bier_controls").equals("")) {
                controlsMAFs.put("BIER", options.get("maf_bier_controls"));
            }

            if (options.containsKey("exc_evs_controls") && options.get("exc_evs_controls").equalsIgnoreCase("on")) {
                whereClauses.add("(key NOT LIKE 'EVS%' OR key is null)");
            } else if (options.containsKey("maf_evs_controls") && !options.get("maf_evs_controls").equals("")) {
                controlsMAFs.put("BIER", options.get("maf_evs_controls"));
            }


            if (options.containsKey("conseq_type[]") && !options.get("conseq_type[]").equals("")) {
                whereClauses.add(processConseqType(options.get("conseq_type[]")));
            }

            if (options.containsKey("genes") && !options.get("genes").equals("")) {
                whereClauses.add(processGeneList(options.get("genes")));
//                processGeneList(options.get("genes"));
            }

            if (sampleGenotypes.size() > 0) {
                StringBuilder sg = new StringBuilder();
                int csg = 0;
                sg.append("(");
                for (Map.Entry<String, List<String>> entry : sampleGenotypes.entrySet()) {
                    sg.append("(");
                    sg.append("sample_name='").append(entry.getKey()).append("' AND (");

                    for (int i = 0; i < entry.getValue().size(); i++) {
                        String[] aux = entry.getValue().get(i).split("/");
                        sg.append("(");
                        sg.append("allele_1=").append(aux[0]).append(" AND allele_2=").append(aux[1]);
                        sg.append(")");

                        if (i + 1 < entry.getValue().size()) {
                            sg.append(" OR ");
                        }
                    }

                    sg.append(")");

                    sg.append(" OR sample_name<>'").append(entry.getKey()).append("'");


                    sg.append(")");

                    if (csg + 1 < sampleGenotypes.entrySet().size()) {
                        sg.append(" AND ");
                    }
                    csg++;
                }
                sg.append(")");
                System.out.println(sg);
                whereClauses.add(sg.toString());
            }


            String sql = "SELECT count(*) as count FROM sample ;";

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            int numSamples = 0;

            while (rs.next()) {

                numSamples = rs.getInt("count");
            }

            stmt.close();


            System.out.println("controlsMAFs = " + controlsMAFs);


            System.out.println("sampleGenotypes = " + sampleGenotypes);

            String innerJoinVariantSQL = " left join variant_info on variant.id_variant=variant_info.id_variant ";
            String innerJoinEffectSQL = " inner join variant_effect on variant_effect.chromosome=variant.chromosome AND variant_effect.position=variant.position AND variant_effect.reference_allele=variant.ref AND variant_effect.alternative_allele = variant.alt ";


            sql = "SELECT distinct variant.genes,variant.consequence_types, variant.id_variant, variant_info.key, variant_info.value, sample_info.sample_name, sample_info.allele_1, sample_info.allele_2, variant_stats.chromosome ," +
                    "variant_stats.position , variant_stats.allele_ref , variant_stats.allele_alt , variant_stats.id , variant_stats.maf , variant_stats.mgf, " +
                    "variant_stats.allele_maf , variant_stats.genotype_maf , variant_stats.miss_allele , variant_stats.miss_gt , variant_stats.mendel_err ," +
                    "variant_stats.is_indel , variant_stats.cases_percent_dominant , variant_stats.controls_percent_dominant , variant_stats.cases_percent_recessive , variant_stats.controls_percent_recessive " + //, variant_stats.genotypes  " +
                    " FROM variant_stats " +
                    "inner join variant on variant_stats.chromosome=variant.chromosome AND variant_stats.position=variant.position AND variant_stats.allele_ref=variant.ref AND variant_stats.allele_alt=variant.alt " +
                    //innerJoinEffectSQL +
                    "inner join sample_info on variant.id_variant=sample_info.id_variant " +
                    innerJoinVariantSQL;

            if (whereClauses.size() > 0) {
                StringBuilder where = new StringBuilder(" where ");

                for (int i = 0; i < whereClauses.size(); i++) {
                    where.append(whereClauses.get(i));
                    if (i < whereClauses.size() - 1) {
                        where.append(" AND ");
                    }
                }

                sql += where.toString() + " ORDER BY variant_stats.chromosome , variant_stats.position , variant_stats.allele_ref , variant_stats.allele_alt ;";
            }

            System.out.println(sql);

            System.out.println("Start SQL");
            long start = System.currentTimeMillis();
            stmt = con.createStatement();

            rs = stmt.executeQuery(sql);

            VariantStats vs;
            VariantInfo vi = null;


            String chr = "";
            int pos = 0;
            String ref = "", alt = "";

            System.out.println("End SQL: " + ((System.currentTimeMillis() - start) / 1000.0) + " s.");
            System.out.println("Processing");

            while (rs.next()) {
                if (!rs.getString("chromosome").equals(chr) ||
                        rs.getInt("position") != pos ||
                        !rs.getString("allele_ref").equals(ref) ||
                        !rs.getString("allele_alt").equals(alt)) {


                    chr = rs.getString("chromosome");
                    pos = rs.getInt("position");
                    ref = rs.getString("allele_ref");
                    alt = rs.getString("allele_alt");

                    if (vi != null && filterGenotypes(vi, numSamples) && filterControls(vi, controlsMAFs)) {
                        list.add(vi);
                    }
                    vi = new VariantInfo(chr, pos, ref, alt);
                    vs = new VariantStats(chr, pos, ref, alt,
                            rs.getDouble("maf"), rs.getDouble("mgf"), rs.getString("allele_maf"), rs.getString("genotype_maf"), rs.getInt("miss_allele"),
                            rs.getInt("miss_gt"), rs.getInt("mendel_err"), rs.getInt("is_indel") == 1, rs.getDouble("cases_percent_dominant"), rs.getDouble("controls_percent_dominant"),
                            rs.getDouble("cases_percent_recessive"), rs.getDouble("controls_percent_recessive"));
                    vs.setId(rs.getString("id"));

                    // vi.addGenotypes(rs.getString("genotypes"));

                    vi.addStats(vs);
                    vi.addGenes(rs.getString("genes"));
                    vi.addConsequenceTypes(rs.getString("consequence_types"));
                }

                if (rs.getString("key") != null && rs.getString("value") != null) {

                    vi.addControl(rs.getString("key"), rs.getString("value"));
                }


                String sample = rs.getString("sample_name");
                String gt = rs.getInt("allele_1") + "/" + rs.getInt("allele_2");

                vi.addSammpleGenotype(sample, gt);
                // vi.addGeneAndConsequenceType(rs.getString("gene_name"), rs.getString("consequence_type_obo"));

            }

            if (vi != null && filterGenotypes(vi, numSamples) && filterControls(vi, controlsMAFs)) {
                list.add(vi);
            }
            stmt.close();


            System.out.println("Total: (" + list.size() + ")");
            System.out.println("End processing: " + ((System.currentTimeMillis() - start) / 1000.0) + " s.");

            con.close();

        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("STATS: " + e.getClass().getName() + ": " + e.getMessage());
        }

        return list;
    }

    private void showDb(String dbName) {
        System.out.println("DB: " + dbName);
    }

    @Override
    public List<VariantStats> getRecordsStats(Map<String, String> options) {

        Connection con;
        Statement stmt;
        List<VariantStats> list = new ArrayList<>(100);

        String dbName = options.get("db_name");

        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            List<String> whereClauses = new ArrayList<>(10);

            if (options.containsKey("region_list") && !options.get("region_list").equals("")) {

                StringBuilder regionClauses = new StringBuilder("(");
                String[] regions = options.get("region_list").split(",");
                Pattern pattern = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
                Matcher matcher;


                for (int i = 0; i < regions.length; i++) {
                    String region = regions[i];
                    matcher = pattern.matcher(region);
                    if (matcher.find()) {
                        String chr = matcher.group(1);
                        int start = Integer.valueOf(matcher.group(2));
                        int end = Integer.valueOf(matcher.group(3));

                        regionClauses.append("( variant_stats.chromosome='").append(chr).append("' AND ");
                        regionClauses.append("variant_stats.position>=").append(start).append(" AND ");
                        regionClauses.append("variant_stats.position<=").append(end).append(" )");


                        if (i < (regions.length - 1)) {
                            regionClauses.append(" OR ");

                        }

                    }
                }
                regionClauses.append(" ) ");
                whereClauses.add(regionClauses.toString());
            }

            if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
                String val = options.get("mend_error");
                String opt = options.get("option_mend_error");
                whereClauses.add("variant_stats.mendel_err " + opt + " " + val);

            }

            if (options.containsKey("is_indel") && options.get("is_indel").equalsIgnoreCase("on")) {
                whereClauses.add("variant_stats.is_indel=1");
            }

            if (options.containsKey("maf") && !options.get("maf").equals("")) {
                String val = options.get("maf");
                String opt = options.get("option_maf");
                whereClauses.add("variant_stats.maf " + opt + " " + val);

            }

            if (options.containsKey("mgf") && !options.get("mgf").equals("")) {
                String val = options.get("mgf");
                String opt = options.get("option_mgf");
                whereClauses.add("variant_stats.mgf " + opt + " " + val);

            }

            if (options.containsKey("miss_allele") && !options.get("miss_allele").equals("")) {
                String val = options.get("miss_allele");
                String opt = options.get("option_miss_allele");
                whereClauses.add("variant_stats.miss_allele " + opt + " " + val);
            }
            if (options.containsKey("miss_gt") && !options.get("miss_gt").equals("")) {
                String val = options.get("miss_gt");
                String opt = options.get("option_miss_gt");
                whereClauses.add("variant_stats.miss_gt " + opt + " " + val);

            }
            if (options.containsKey("cases_percent_dominant") && !options.get("cases_percent_dominant").equals("")) {
                String val = options.get("cases_percent_dominant");
                String opt = options.get("option_cases_dom");
                whereClauses.add("variant_stats.cases_percent_dominant " + opt + " " + val);
            }

            if (options.containsKey("controls_percent_dominant") && !options.get("controls_percent_dominant").equals("")) {
                String val = options.get("controls_percent_dominant");
                String opt = options.get("option_controls_dom");
                whereClauses.add("variant_stats.controls_percent_dominant " + opt + " " + val);
            }

            if (options.containsKey("cases_percent_recessive") && !options.get("cases_percent_recessive").equals("")) {
                String val = options.get("cases_percent_recessive");
                String opt = options.get("option_cases_rec");
                whereClauses.add("variant_stats.cases_percent_recessive " + opt + " " + val);
            }

            if (options.containsKey("controls_percent_recessive") && !options.get("controls_percent_recessive").equals("")) {
                String val = options.get("controls_percent_recessive");
                String opt = options.get("option_controls_rec");
                whereClauses.add("variant_stats.controls_percent_recessive " + opt + " " + val);
            }

            if (options.containsKey("genes") && !options.get("genes").equals("")) {
                whereClauses.add(processGeneList(options.get("genes")));
            }


            String sql = "SELECT distinct variant_stats.chromosome ," +
                    "variant_stats.position , variant_stats.allele_ref , variant_stats.allele_alt, variant_stats.maf , variant_stats.mgf, " +
                    "variant_stats.allele_maf , variant_stats.genotype_maf , variant_stats.miss_allele , variant_stats.miss_gt , variant_stats.mendel_err ," +
                    "variant_stats.is_indel , variant_stats.cases_percent_dominant , variant_stats.controls_percent_dominant , variant_stats.cases_percent_recessive , variant_stats.controls_percent_recessive" +
                    " FROM variant_stats ";

            if (whereClauses.size() > 0) {
                StringBuilder where = new StringBuilder(" where ");

                for (int i = 0; i < whereClauses.size(); i++) {
                    where.append(whereClauses.get(i));
                    if (i < whereClauses.size() - 1) {
                        where.append(" AND ");
                    }
                }

                sql += where.toString() + " ORDER BY variant_stats.chromosome , variant_stats.position , variant_stats.allele_ref ;";
            }

            System.out.println(sql);

            System.out.println("Start SQL");
            long start = System.currentTimeMillis();
            stmt = con.createStatement();

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            VariantStats vs;
            VariantInfo vi = null;


            String chr = "";
            int pos = 0;
            String ref = "", alt = "";
            System.out.println("End SQL: " + ((System.currentTimeMillis() - start) / 1000.0) + " s.");

            System.out.println("Processing");

            while (rs.next()) {

                chr = rs.getString("chromosome");
                pos = rs.getInt("position");
                ref = rs.getString("allele_ref");
                alt = rs.getString("allele_alt");

                vs = new VariantStats(chr, pos, ref, alt,
                        rs.getDouble("maf"), rs.getDouble("mgf"), rs.getString("allele_maf"), rs.getString("genotype_maf"), rs.getInt("miss_allele"),
                        rs.getInt("miss_gt"), rs.getInt("mendel_err"), rs.getInt("is_indel") == 1, rs.getDouble("cases_percent_dominant"), rs.getDouble("controls_percent_dominant"),
                        rs.getDouble("cases_percent_recessive"), rs.getDouble("controls_percent_recessive"));

                list.add(vs);

            }

            System.out.println("Total: (" + list.size() + ")");
            System.out.println("End processing: " + ((System.currentTimeMillis() - start) / 1000.0) + " s.");


            stmt.close();
            con.close();

        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("STATS: " + e.getClass().getName() + ": " + e.getMessage());
        }

        return list;
    }

    @Override
    public List<VariantEffect> getEffect(Map<String, String> options) {

        Statement stmt;
        Connection con;
        List<VariantEffect> list = new ArrayList<>(100);

        String dbName = options.get("db_name");

        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            String chr = options.get("chr");
            int pos = Integer.valueOf(options.get("pos"));
            String ref = options.get("ref");
            String alt = options.get("alt");


            String sql = "SELECT * FROM variant_effect WHERE chromosome='" + chr + "' AND position=" + pos + " AND reference_allele='" + ref + "' AND alternative_allele='" + alt + "';";

            System.out.println(sql);

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            VariantEffect ve;


            while (rs.next()) {
                ve = new VariantEffect(rs.getString("chromosome"), rs.getInt("position"), rs.getString("reference_allele"), rs.getString("alternative_allele"),
                        rs.getString("feature_id"), rs.getString("feature_name"), rs.getString("feature_type"), rs.getString("feature_biotype"),
                        rs.getString("feature_chromosome"), rs.getInt("feature_start"), rs.getInt("feature_end"), rs.getString("feature_strand"),
                        rs.getString("snp_id"), rs.getString("ancestral"), rs.getString("alternative"), rs.getString("gene_id"), rs.getString("transcript_id"),
                        rs.getString("gene_name"), rs.getString("consequence_type"), rs.getString("consequence_type_obo"), rs.getString("consequence_type_desc"),
                        rs.getString("consequence_type_type"), rs.getInt("aa_position"), rs.getString("aminoacid_change"), rs.getString("codon_change"));
                list.add(ve);

            }

            stmt.close();
            con.close();

        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("EFFECT: " + e.getClass().getName() + ": " + e.getMessage());
        }


        return list;
    }

    @Override
    public VariantAnalysisInfo getAnalysisInfo(Map<String, String> options) {

        Statement stmt;
        Connection con;
        VariantAnalysisInfo vi = new VariantAnalysisInfo();

        String dbName = options.get("db_name");

        showDb(dbName);

        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            String sql = "SELECT * FROM sample ;";

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {

                vi.addSample(rs.getString("name"));

            }

            stmt.close();

            sql = "select * from consequence_type_count";
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);


            while (rs.next()) {

                vi.addConsequenceType(rs.getString("consequence_type_obo"), rs.getInt("count"));

            }

            stmt.close();


            sql = "select * from biotype_count;";
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {

                vi.addBiotype(rs.getString("feature_biotype"), rs.getInt("count"));

            }

            stmt.close();

            sql = "select * from global_stats";
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {

                vi.addGlobalStats(rs.getString("name").toLowerCase(), rs.getDouble("value"));

            }

            stmt.close();

            sql = "select count(*) as count, chromosome from variant group by chromosome";
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);

            while (rs.next()) {

                vi.addChromosome(rs.getString("chromosome"), rs.getInt("count"));

            }

            stmt.close();
            con.close();

        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("ANALYSIS INFO: " + e.getClass().getName() + ": " + e.getMessage());
        }

        return vi;
    }

    private String processGeneList(String genes) {
        System.out.println("genes = " + genes);
        List<String> list = new ArrayList<>();

//        Client client = ClientBuilder.newClient();
//        WebTarget webTarget = client.target("http://ws.bioinfo.cipf.es/cellbase/rest/latest/hsa/feature/gene/");

        Client client = Client.create();
        WebResource webResource = client.resource("http://ws.bioinfo.cipf.es/cellbase/rest/latest/hsa/feature/gene/");

        ObjectMapper mapper = new ObjectMapper();

//        Response response = webTarget.path(genes).path("info").queryParam("of", "json").request().get();
        String response = webResource.path(genes).path("info").queryParam("of", "json").get(String.class);
        String data = response.toString();

        System.out.println("response = " + response);


        try {
            JsonNode actualObj = mapper.readTree(data);
            Iterator<JsonNode> it = actualObj.iterator();
            Iterator<JsonNode> aux;
            StringBuilder sb;

            while (it.hasNext()) {
                JsonNode node = it.next();
                if (node.isArray()) {

                    aux = node.iterator();
                    while (aux.hasNext()) {
                        JsonNode auxNode = aux.next();
                        sb = new StringBuilder("(");

                        System.out.println("auxNode.get(\"chromosome\").asText() = " + auxNode.get("chromosome").asText());

                        sb.append("variant_stats.chromosome='").append(auxNode.get("chromosome").asText()).append("' AND ");
                        sb.append("variant_stats.position>=").append(auxNode.get("start")).append(" AND ");
                        sb.append("variant_stats.position<=").append(auxNode.get("end")).append(" )");

                        list.add(sb.toString());
                    }

                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        String res = "(" + StringUtils.join(list, " OR ") + ")";

        return res;

    }

    private boolean filterControls(VariantInfo vi, Map<String, String> controlsMAFs) {
        boolean res = true;

        String key;
        VariantControl vc;
        float controlMAF;

        for (Map.Entry<String, VariantControl> entry : vi.getControls().entrySet()) {

            key = entry.getKey();
            vc = entry.getValue();

            if (controlsMAFs.containsKey(key)) {

                controlMAF = Float.valueOf(controlsMAFs.get(key));
                if (vc.getMaf() > controlMAF) {
                    return false;
                }

            }
        }
        return res;
    }

    private String processConseqType(String conseqType) {
        List<String> clauses = new ArrayList<>(10);

        String[] cts = conseqType.split(",");

        for (String ct : cts) {
            clauses.add("(variant.consequence_types LIKE '%" + ct + "%' )");
        }

        String res = "";
        if (clauses.size() > 0) {
            res = "(" + StringUtils.join(clauses, " OR ") + ")";
        }


        return res;
    }

    private boolean filterGenotypes(VariantInfo variantInfo, int numSamples) {
//        if (variantInfo.getSampleGenotypes().size() != numSamples) {
//            return false;
//        } else {
//            return true;
//        }
        return variantInfo.getSampleGenotypes().size() == numSamples;

    }

    private Map<String, List<String>> processSamplesGT(Map<String, String> options) {
        Map<String, List<String>> samplesGenotypes = new LinkedHashMap<>(10);
        List<String> genotypesList;

        String key, val;
        for (Map.Entry<String, String> entry : options.entrySet()) {
            key = entry.getKey();
            val = entry.getValue();

            if (key.startsWith("sampleGT_")) {
                String sampleName = key.replace("sampleGT_", "").replace("[]", "");
                String[] genotypes = val.split(",");

                if (samplesGenotypes.containsKey(sampleName)) {
                    genotypesList = samplesGenotypes.get(sampleName);
                } else {

                    genotypesList = new ArrayList<>();
                    samplesGenotypes.put(sampleName, genotypesList);
                }


                for (int i = 0; i < genotypes.length; i++) {

                    genotypesList.add(genotypes[i]);
                }

            }

        }
        return samplesGenotypes;
    }

    private void processSamplesGT(Map<String, String> options, List<String> whereClauses) {
        String key, val;

        List<String> auxClauses = new ArrayList<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            key = entry.getKey();
            val = entry.getValue();

            if (key.startsWith("sampleGT_")) {
                String sampleName = key.replace("sampleGT_", "").replace("[]", "");
                String[] genotypes = val.split(",");
                StringBuilder sb = new StringBuilder("(");


                for (int i = 0; i < genotypes.length; i++) {
                    String[] gt = genotypes[i].split("_");

                    sb.append("(");
                    sb.append("sample_info.sample_name='" + sampleName + "'");
                    sb.append(" AND sample_info.allele_1=" + gt[0]);
                    sb.append(" AND sample_info.allele_2=" + gt[1]);

                    sb.append(")");

                    if (i < genotypes.length - 1) {
                        sb.append(" OR ");
                    }
                }
                sb.append(")");
                auxClauses.add(sb.toString());
            }

        }

        if (auxClauses.size() > 0) {
            String finalSampleWhere = StringUtils.join(auxClauses, " AND ");

            whereClauses.add(finalSampleWhere);

        }

    }

    /* ******************************************
     *          Path and index checking         *
     * ******************************************/

    private Path getMetaDir(Path file) {
        String inputName = file.getFileName().toString();
        return file.getParent().resolve(".meta_" + inputName);
    }



}
