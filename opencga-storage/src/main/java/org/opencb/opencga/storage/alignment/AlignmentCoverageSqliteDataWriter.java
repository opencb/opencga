package org.opencb.opencga.storage.alignment;

import org.opencb.commons.bioformats.alignment.stats.AlignmentCoverage;
import org.opencb.commons.bioformats.alignment.io.writers.coverage.AlignmentCoverageDataWriter;
import org.opencb.commons.db.SqliteSingletonConnection;

import java.lang.Override;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jcoll
 * Date: 12/4/13
 * Time: 6:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class AlignmentCoverageSqliteDataWriter implements AlignmentCoverageDataWriter {

    private Statement stmt;
    private PreparedStatement pstmt;
    private SqliteSingletonConnection connection;


    public AlignmentCoverageSqliteDataWriter(String dbName) {
        this.stmt = null;
        this.pstmt = null;
        this.connection = new SqliteSingletonConnection(dbName);
    }

    @Override
    public boolean write(AlignmentCoverage alignmentCoverage) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
    @Override
    public boolean write(List<AlignmentCoverage> batch) {

        boolean res = true;
        String sql = "INSERT INTO coverage (chromosome, start,end,coverage) VALUES(?,?,?,?);";
        try {
            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);

            for(AlignmentCoverage align: batch){

                pstmt.setString(1,align.getChromosome());
                pstmt.setInt(2, align.getStart());
                pstmt.setInt(3, align.getEnd());
                pstmt.setDouble(4, align.getCoverage());
                pstmt.execute();

            }

            pstmt.close();
            SqliteSingletonConnection.getConnection().commit();

        } catch (SQLException e) {
            System.err.println("SAMPLE: " + e.getClass().getName() + ": " + e.getMessage());
             res = false;
        }

        return res;
    }

    @Override
    public boolean open() {
        return SqliteSingletonConnection.getConnection() != null;
    }

    @Override
    public boolean close() {
        boolean res = true;
        try {
            SqliteSingletonConnection.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
            res = false;
        }
        return res;
    }

    @Override
    public boolean pre() {
        boolean res = true;

        String coverageTable  = "CREATE TABLE IF NOT EXISTS coverage(" +
                "id_coverage INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "chromosome TEXT, " +
                "start INTEGER, " +
                "end INTEGER, "+
                "coverage DOUBLE);";

        try {
            stmt = SqliteSingletonConnection.getConnection().createStatement();
            stmt.execute(coverageTable);
            stmt.close();
            SqliteSingletonConnection.getConnection().commit();

        } catch (SQLException e) {
            System.err.println("PRE: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }

    @Override
    public boolean post() {

        boolean res = true;

        try {
            stmt = SqliteSingletonConnection.getConnection().createStatement();
            stmt.execute("CREATE INDEX IF NOT EXISTS coverage_chromosome_start_end ON coverage (chromosome, start,end);");
            stmt.close();
            SqliteSingletonConnection.getConnection().commit();

        } catch (SQLException e) {
            System.err.println("POST: " + e.getClass().getName() + ": " + e.getMessage());
            res = false;
        }

        return res;
    }




}
