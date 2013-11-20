package org.opencb.opencga.storage.variant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStat;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: jrodriguez
 * Date: 11/15/13
 * Time: 12:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class VariantMonbaseQueryMaker implements VariantQueryMaker{
    private List<Scan> regionScans;
    private List<ResultScanner> regionResults;
    private List<ResultScanner> regionEffect;
    private String study;
    private String tableName;
    private HTable table;
    private HTable effectTable;
    private Configuration config;
    private HBaseAdmin admin;

    /*@Override
    public List<VariantInfo> getRecords(Map<String, String> options) {

        List<VariantInfo> list = new ArrayList<>(100);

        study = "whatever";
        tableName = "prueba7";

        //String dbName = options.get("db_name");

        regionScans = new ArrayList<>();

        try {
            // HBase configuration and inicialization
            config = HBaseConfiguration.create();
            config.set("hbase.master", "172.24.79.30:60010");
            config.set("hbase.zookeeper.quorum", "172.24.79.30");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            admin = new HBaseAdmin(config);
            table = new HTable(config, tableName);
            effectTable = new HTable(config, tableName + "effect");

            List<String> whereClauses = new ArrayList<>(10);

            Map<String, List<String>> sampleGenotypes;
            Map<String, String> controlsMAFs = new LinkedHashMap<>();

            // Region filters
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
                        String start = matcher.group(2);
                        String end = matcher.group(3);
                        start = buildRowkey(chr, start);
                        end = buildRowkey(chr, end);
                        Scan regionScan = new Scan(start.getBytes(), end.getBytes());
                        regionScans.add(regionScan);
                    }
                }
            }

            if (options.containsKey("chr_pos") && !options.get("chr_pos").equals("")) {
                String chr = options.get("chr_pos");
                String start = options.get("start_pos");
                String end = options.get("end_pos");
                Scan regionScan = new Scan();
                regionScans.add(regionScan);
                if (options.containsKey("start_pos") && !options.get("start_pos").equals("")) {
                    start = buildRowkey(chr, start);
                    regionScan.setStartRow(start.getBytes());
                }

                if (options.containsKey("end_pos") && !options.get("end_pos").equals("")) {
                    end = buildRowkey(chr, end);
                    regionScan.setStopRow(end.getBytes());
                }
                regionScans.add(regionScan);
            }

            // Result extraction and protobuf decoding.
            for(Scan region: regionScans){
                ResultScanner resultScan = table.getScanner(region);
                ResultScanner resultEffect = effectTable.getScanner(region);
                regionResults.add(resultScan);
                regionEffect.add(resultEffect);
            }

            for(ResultScanner regionRes : regionResults){
                for(Result r : regionRes){
                    List <VariantFieldsProtos.VariantSample> samples = new LinkedList<>();
                    NavigableMap<byte[], byte[]> infoMap = r.getFamilyMap("i".getBytes());
                    byte[] byteStats = infoMap.get((study + "_" + "stats").getBytes());
                    VariantFieldsProtos.VariantStats stats = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
                    byte[] byteInfo = infoMap.get((study + "_" + "info").getBytes());
                    VariantFieldsProtos.VariantInfo info = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
                    if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
                        String val = options.get("mend_error");
                        String opt = options.get("option_mend_error");
                        switch (opt){
                            case "=":  if(!(stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMendelianErrors() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMendelianErrors() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMendelianErrors() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMendelianErrors() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("is_indel") && options.get("is_indel").equalsIgnoreCase("on")) {
                        if(!stats.getIsIndel()){
                            continue;
                        }
                    }
                    if (options.containsKey("maf") && !options.get("maf").equals("")) {
                        String val = options.get("maf");
                        String opt = options.get("option_maf");
                        switch (opt){
                            case "=":  if(!(stats.getMaf() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMaf() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMaf() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMaf() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMaf() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMaf() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("mgf") && !options.get("mgf").equals("")) {
                        String val = options.get("mgf");
                        String opt = options.get("option_mgf");
                        switch (opt){
                            case "=":  if(!(stats.getMgf() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMgf() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMgf() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMgf() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMgf() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMgf() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("miss_allele") && !options.get("miss_allele").equals("")) {
                        String val = options.get("miss_allele");
                        String opt = options.get("option_miss_allele");
                        switch (opt){
                            case "=":  if(!(stats.getMissingAlleles() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMissingAlleles() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMissingAlleles() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMissingAlleles()>= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMissingAlleles() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMissingAlleles()== Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("miss_gt") && !options.get("miss_gt").equals("")) {
                        String val = options.get("miss_gt");
                        String opt = options.get("option_miss_gt");
                        switch (opt){
                            case "=":  if(!(stats.getMissingGenotypes() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMissingGenotypes() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMissingGenotypes() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMissingGenotypes()>= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMissingGenotypes() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMissingGenotypes()== Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("cases_percent_dominant") && !options.get("cases_percent_dominant").equals("")) {
                        String val = options.get("cases_percent_dominant");
                        String opt = options.get("option_cases_dom");
                        switch (opt){
                            case "=":  if(!(stats.getCasesPercentDominant() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getCasesPercentDominant() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getCasesPercentDominant() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getCasesPercentDominant() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getCasesPercentDominant() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getCasesPercentDominant() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("controls_percent_dominant") && !options.get("controls_percent_dominant").equals("")) {
                        String val = options.get("controls_percent_dominant");
                        String opt = options.get("option_controls_dom");
                        switch (opt){
                            case "=":  if(!(stats.getControlsPercentDominant() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getControlsPercentDominant() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getControlsPercentDominant() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getControlsPercentDominant() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getControlsPercentDominant() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getControlsPercentDominant() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("cases_percent_recessive") && !options.get("cases_percent_recessive").equals("")) {
                        String val = options.get("cases_percent_recessive");
                        String opt = options.get("option_cases_rec");
                        switch (opt){
                            case "=":  if(!(stats.getCasesPercentRecessive() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getCasesPercentRecessive() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getCasesPercentRecessive() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getCasesPercentRecessive() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getCasesPercentRecessive() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getCasesPercentRecessive() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("controls_percent_recessive") && !options.get("controls_percent_recessive").equals("")) {
                        String val = options.get("controls_percent_recessive");
                        String opt = options.get("option_controls_rec");
                        switch (opt){
                            case "=":  if(!(stats.getControlsPercentRecessive() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getControlsPercentRecessive() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getControlsPercentRecessive() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getControlsPercentRecessive() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getControlsPercentRecessive() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getControlsPercentRecessive() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
                        String val = options.get("mend_error");
                        String opt = options.get("option_mend_error");
                        switch (opt){
                            case "=":  if(!(stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMendelianErrors() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMendelianErrors() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMendelianErrors() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMendelianErrors() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
                        String val = options.get("mend_error");
                        String opt = options.get("option_mend_error");
                        switch (opt){
                            case "=":  if(!(stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMendelianErrors() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMendelianErrors() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMendelianErrors() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMendelianErrors() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                        }
                    }
                    if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
                        String val = options.get("mend_error");
                        String opt = options.get("option_mend_error");
                        switch (opt){
                            case "=":  if(!(stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                            case ">":  if(!(stats.getMendelianErrors() > Integer.valueOf(val))){continue;}
                            case "<":  if(!(stats.getMendelianErrors() < Integer.valueOf(val))){continue;}
                            case ">=": if(!(stats.getMendelianErrors() >= Integer.valueOf(val))){continue;}
                            case "<=": if(!(stats.getMendelianErrors() <= Integer.valueOf(val))){continue;}
                            case "!=": if((stats.getMendelianErrors() == Integer.valueOf(val))){continue;}
                        }
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
                    NavigableMap<byte[], byte[]> dataMap = r.getFamilyMap("d".getBytes());
                    for(Map.Entry<byte[], byte[]> qualifier : dataMap.entrySet()){
                        String qual = new String(qualifier.getKey(), "US-ASCII");
                        VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(qualifier.getValue());
                        samples.add(sample);
                    }
                }
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

 *//*           if (options.containsKey("exc_1000g_controls") && options.get("exc_1000g_controls").equalsIgnoreCase("on")) {
                whereClauses.add("(key NOT LIKE '1000G%' OR key is null)");
            } else if (options.containsKey("maf_1000g_controls") && !options.get("maf_1000g_controls").equals("")) {
                controlsMAFs.put("1000G", options.get("maf_1000g_controls"));
            }


            if (options.containsKey("exc_bier_controls") && options.get("exc_bier_controls").equalsIgnoreCase("on")) {
                whereClauses.add("(key NOT LIKE 'BIER%' OR key is null)");
            } else if (options.containsKey("maf_bier_controls") && !options.get("maf_bier_controls").equals("")) {
                controlsMAFs.put("BIER", options.get("maf_bier_controls"));
            }*//*


            if (options.containsKey("conseq_type[]") && !options.get("conseq_type[]").equals("")) {
                whereClauses.add(processConseqType(options.get("conseq_type[]")));
            }

            if (options.containsKey("genes") && !options.get("genes").equals("")) {
                whereClauses.add(processGeneList(options.get("genes")));
//                processGeneList(options.get("genes"));
            }


            System.out.println("controlsMAFs = " + controlsMAFs);

            sampleGenotypes = processSamplesGT(options);

            System.out.println("sampleGenotypes = " + sampleGenotypes);

            String innerJoinVariantSQL = " left join variant_info on variant.id_variant=variant_info.id_variant ";
            String innerJoinEffectSQL = " inner join variant_effect on variant_effect.chromosome=variant.chromosome AND variant_effect.position=variant.position AND variant_effect.reference_allele=variant.ref AND variant_effect.alternative_allele = variant.alt ";


            String sql = "SELECT distinct variant_effect.gene_name,variant_effect.consequence_type_obo, variant.id_variant, variant_info.key, variant_info.value, sample_info.sample_name, sample_info.allele_1, sample_info.allele_2, variant_stats.chromosome ," +
                    "variant_stats.position , variant_stats.allele_ref , variant_stats.allele_alt , variant_stats.id , variant_stats.maf , variant_stats.mgf, " +
                    "variant_stats.allele_maf , variant_stats.genotype_maf , variant_stats.miss_allele , variant_stats.miss_gt , variant_stats.mendel_err ," +
                    "variant_stats.is_indel , variant_stats.cases_percent_dominant , variant_stats.controls_percent_dominant , variant_stats.cases_percent_recessive , variant_stats.controls_percent_recessive " + //, variant_stats.genotypes  " +
                    " FROM variant_stats " +
                    "inner join variant on variant_stats.chromosome=variant.chromosome AND variant_stats.position=variant.position AND variant_stats.allele_ref=variant.ref AND variant_stats.allele_alt=variant.alt " +
                    innerJoinEffectSQL +
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

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            VcfVariantStat vs;
            VariantInfo vi = null;


            String chr = "";
            int pos = 0;
            String ref = "", alt = "";

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

                    if (vi != null && filterGenotypes(vi, sampleGenotypes) && filterControls(vi, controlsMAFs)) {
                        list.add(vi);
                    }
                    vi = new VariantInfo(chr, pos, ref, alt);
                    vs = new VcfVariantStat(chr, pos, ref, alt,
                            rs.getDouble("maf"), rs.getDouble("mgf"), rs.getString("allele_maf"), rs.getString("genotype_maf"), rs.getInt("miss_allele"),
                            rs.getInt("miss_gt"), rs.getInt("mendel_err"), rs.getInt("is_indel"), rs.getDouble("cases_percent_dominant"), rs.getDouble("controls_percent_dominant"),
                            rs.getDouble("cases_percent_recessive"), rs.getDouble("controls_percent_recessive"));
                    vs.setId(rs.getString("id"));

                    // vi.addGenotypes(rs.getString("genotypes"));

                    vi.addStats(vs);
                }

                if (rs.getString("key") != null && rs.getString("value") != null) {

                    vi.addControl(rs.getString("key"), rs.getString("value"));
                }


                String sample = rs.getString("sample_name");
                String gt = rs.getInt("allele_1") + "/" + rs.getInt("allele_2");

                vi.addSammpleGenotype(sample, gt);
                vi.addGeneAndConsequenceType(rs.getString("gene_name"), rs.getString("consequence_type_obo"));


            }

            System.out.println("End processing");
            if (vi != null && filterGenotypes(vi, sampleGenotypes) && filterControls(vi, controlsMAFs)) {
                list.add(vi);
            }
            table.close();
            admin.close();

        } catch (ClassNotFoundException | SQLException | ZooKeeperConnectionException | MasterNotRunningException | IOException e) {
            System.err.println("STATS: " + e.getClass().getName() + ": " + e.getMessage());
        }

        return list;
    }
    }*/

    @Override
    public List<VariantInfo> getRecords(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<VariantStat> getRecordsStats(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<VariantEffect> getEffect(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public VariantAnalysisInfo getAnalysisInfo(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private String buildRowkey(String chromosome, String position) {
        if (chromosome.length() < 2) {
            chromosome = "0" + chromosome;
        }
        if (position.length() < 10) {
            while (position.length() < 10) {
                position = "0" + position;
            }
        }
        return chromosome + "_" + position;
    }
}
