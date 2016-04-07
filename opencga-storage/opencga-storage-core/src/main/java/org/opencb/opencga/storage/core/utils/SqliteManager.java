/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.utils;

import org.opencb.opencga.core.common.XObject;
import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class SqliteManager {

    private int LIMITROWS = 100000;
    private Connection connection;

    private Map<String, Integer> tableInsertCounters;
    private Map<String, PreparedStatement> tableInsertPreparedStatement;
    private Map<String, PreparedStatement> tableUpdatePreparedStatement;

    private Map<String, XObject> tableColumns;

    public SqliteManager() {

        tableInsertCounters = new HashMap<>();
        tableInsertPreparedStatement = new HashMap<>();
        tableUpdatePreparedStatement = new HashMap<>();
        tableColumns = new HashMap<>();
    }

    public void connect(Path filePath, boolean readOnly) throws ClassNotFoundException, SQLException {
        Path dbPath = Paths.get(filePath.toString());

        SQLiteConfig config = new SQLiteConfig();
//        config.setLockingMode(SQLiteConfig.LockingMode.NORMAL);
        if (readOnly) {
            config.setReadOnly(true);
        }

        try {
            Class.forName("org.sqlite.JDBC").newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            System.out.println("Could not find sqlite JDBC");
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toString(), config.toProperties());
        connection.setAutoCommit(false); //Set false to perform commits manually and increase performance on insertion
    }

    public void disconnect(boolean commit) throws SQLException {
        if (connection != null) {
            if (commit) {
                for (String tableName : tableInsertPreparedStatement.keySet()) {
                    tableInsertPreparedStatement.get(tableName).executeBatch();
                }
                for (String tableName : tableUpdatePreparedStatement.keySet()) {
                    tableUpdatePreparedStatement.get(tableName).executeBatch();
                }
                connection.commit();
            }
            connection.close();
        }
    }

    public void createTable(String tableName, XObject columns) throws SQLException {
        Statement createTables = connection.createStatement();

        StringBuilder sbQuery = new StringBuilder();
        sbQuery.append("CREATE TABLE if not exists " + tableName + "(");

        Set<String> names = columns.keySet();
        for (String colName : names) {
            sbQuery.append("'" + colName + "' " + columns.getString(colName) + ",");
        }
        sbQuery.deleteCharAt(sbQuery.length() - 1);
        sbQuery.append(")");

        System.out.println(sbQuery.toString());
        createTables.executeUpdate(sbQuery.toString());

        tableColumns.put(tableName, columns);
    }

    public void createIndex(String tableName, String indexName, XObject indices) throws SQLException {
        Statement createIndex = connection.createStatement();
        StringBuilder sbQuery = new StringBuilder();
        Set<String> names = indices.keySet();
        for (String colName : names) {
            sbQuery.append(colName + ",");
        }
        sbQuery.deleteCharAt(sbQuery.length() - 1);
        String sQuery = "CREATE INDEX " + tableName + "_" + indexName + "_idx on " + tableName + "(" + sbQuery.toString() + ")";
        System.out.println(sQuery);
        createIndex.executeUpdate(sQuery);
        System.out.println("columns created.");

        connection.commit();
    }

    public void commit(String tableName) throws SQLException {
        tableInsertPreparedStatement.get(tableName).executeBatch();
        connection.commit();
    }

    public void insert(XObject xObject, String tableName) throws SQLException {
//        int BatchCount = 0;
        if (!tableInsertCounters.containsKey(tableName)) {
            tableInsertCounters.put(tableName, 0);
        }

        PreparedStatement ps;
        if (!tableInsertPreparedStatement.containsKey(tableName)) {
            StringBuilder sbQuery = new StringBuilder();
            sbQuery.append("INSERT INTO " + tableName + "(");

            Set<String> names = xObject.keySet();
            for (String colName : names) {
                sbQuery.append("'" + colName + "',");
            }

            sbQuery.deleteCharAt(sbQuery.length() - 1);
            sbQuery.append(")values (");
            sbQuery.append(repeat("?,", names.size()));
            sbQuery.deleteCharAt(sbQuery.length() - 1);
            sbQuery.append(")");
            System.out.println(sbQuery.toString());

            ps = connection.prepareStatement(sbQuery.toString());
            tableInsertPreparedStatement.put(tableName, ps);
        } else {
            ps = tableInsertPreparedStatement.get(tableName);
        }

        // everything is ready to insert
        insertByType(ps, xObject, tableColumns.get(tableName));

        //commit batch
        if (tableInsertCounters.get(tableName) % LIMITROWS == 0 && tableInsertCounters.get(tableName) != 0) {
            commit(tableName);
        }
        ps.addBatch();
        tableInsertCounters.put(tableName, tableInsertCounters.get(tableName) + 1);

    }

    public void update(XObject xObject, XObject newXObject, String tableName) throws SQLException {
        PreparedStatement ps;
        if (!tableUpdatePreparedStatement.containsKey(tableName)) {
            StringBuilder sbQuery = new StringBuilder();

            sbQuery.append("UPDATE " + tableName + " SET ");
            Set<String> updateColumns = newXObject.keySet();
            for (String colName : updateColumns) {
                sbQuery.append("'" + colName + "'=?, ");
            }
            sbQuery.delete(sbQuery.length() - 2, sbQuery.length()); //", ".length()
            sbQuery.append(" WHERE ");
            Set<String> whereColumns = xObject.keySet();
            for (String colName : whereColumns) {
                sbQuery.append("'" + colName + "'=? AND ");
            }
            sbQuery.delete(sbQuery.length() - 5, sbQuery.length()); //" AND ".length()

            System.out.println(sbQuery.toString());

            ps = connection.prepareStatement(sbQuery.toString());
            tableUpdatePreparedStatement.put(tableName, ps);
        } else {
            ps = tableUpdatePreparedStatement.get(tableName);
        }

        updateByType(ps, xObject, newXObject, tableColumns.get(tableName));
        //commit batch
        if (tableInsertCounters.get(tableName) % LIMITROWS == 0 && tableInsertCounters.get(tableName) != 0) {
            commit(tableName);
        }
        ps.addBatch();
        tableInsertCounters.put(tableName, tableInsertCounters.get(tableName) + 1);
    }

    public List<XObject> query(String tableName, XObject queryObject) throws SQLException {
//        Statement query = connection.createStatement();
//
//        StringBuilder whereString = new StringBuilder();
//        Set<String> columnNames = queryObject.keySet();
//        for (String colName : columnNames) {
//            switch (queryObject.getString("type").toUpperCase()) {
//                case "INTEGER":
//                case "INT":
//                case "BIGINT":
//                    whereString.append("'" + colName + "'="+queryObject.getString(colName)+" ");
//                break;
//                default:
//                    whereString.append("'" + colName + "'='"+queryObject.getString(colName)+"' ");
//            }
//        }
//        whereString.deleteCharAt(whereString.length() - 1);
//
//        String queryString = "SELECT * FROM " + tableName + " WHERE "+whereString;
//        System.out.println(queryString);
//
//        List<XObject> results = new ArrayList<>();
//        ResultSet rs = query.executeQuery(queryString);
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int columnCount = rsmd.getColumnCount();
//
//
//        while (rs.next()) {
//            XObject row = new XObject();
//            for(int i=1; i<=columnCount; i++){
//                rsmd.getColumnName(i);
//                row.put(rsmd.getColumnName(i),rs.getString(i));
//            }
//
////            results.add(rs.getLong(4));
//        }
//
        return null;
    }

    public List<XObject> query(String queryString) throws SQLException {
        System.out.println(queryString);
        Statement query = connection.createStatement();

        List<XObject> results = new ArrayList<>();
        long tq = System.currentTimeMillis();
        ResultSet rs = query.executeQuery(queryString);
        System.out.println("SQLITE MANAGER Query time " + (System.currentTimeMillis() - tq) + "ms");

        long tx0 = System.currentTimeMillis();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        System.out.println("SQLITE MANAGER Getting Metadata " + (System.currentTimeMillis() - tx0) + "ms");

        long tx = System.currentTimeMillis();
        XObject row;
        while (rs.next()) {
            row = new XObject();
            for (int i = 1; i <= columnCount; i++) {
                row.put(rsmd.getColumnName(i), rs.getString(i));
            }
            results.add(row);
        }
        System.out.println("SQLITE MANAGER Parse to XObject " + (System.currentTimeMillis() - tx) + "ms");
        System.out.println("results size: " + results.size());
        return results;
    }

    public Iterator<XObject> queryIterator(String queryString) throws SQLException {
        System.out.println(queryString);

        Iterator<XObject> it = new QueryIterator(queryString);

        return it;
    }

    private class QueryIterator implements Iterator<XObject> {

        private Statement stmt;
        private ResultSet rs;
        private int columnCount;
        private ResultSetMetaData rsmd;
        private boolean didNext;
        private boolean hasNext;

        public QueryIterator(String queryString) throws SQLException {

            stmt = connection.createStatement();
            rs = stmt.executeQuery(queryString);
            rsmd = rs.getMetaData();
            columnCount = rsmd.getColumnCount();
            didNext = false;
            hasNext = false;
        }

        @Override
        public boolean hasNext() {
            if (!didNext) {
                try {
                    hasNext = rs.next();
                    didNext = true;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (!hasNext) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            return hasNext;
        }

        @Override
        public XObject next() {
            XObject row = null;
            if (!didNext) {
                try {
                    rs.next();

                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            row = new XObject();
            try {
                for (int i = 1; i <= columnCount; i++) {
                    row.put(rsmd.getColumnName(i), rs.getString(i));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            didNext = false;
            return row;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();

        }
    }

    private void insertByType(PreparedStatement ps, XObject xObject, XObject columns) throws SQLException {
        String type;
        int i = 1;
        Set<String> names = xObject.keySet();
        for (String name : names) {
            type = columns.getString(name);
            setByType(ps, i, type, xObject, name);
            i++;
        }
    }

    private void updateByType(PreparedStatement ps, XObject xObject, XObject newXObject, XObject columns) throws SQLException {
        String type;
        int i = 1;
        Set<String> names = newXObject.keySet();
        for (String name : names) {
            type = columns.getString(name);
            setByType(ps, i, type, newXObject, name);
            i++;
        }
        names = xObject.keySet();
        for (String name : names) {
            type = columns.getString(name);
            setByType(ps, i, type, xObject, name);
            i++;
        }
    }

    private void setByType(PreparedStatement ps, int i, String type, XObject xo, String name) throws SQLException {
        //Datatypes In SQLite Version 3 -> http://www.sqlite.org/datatype3.html
        switch (type.toUpperCase()) {
            case "INTEGER":
            case "INT":
                ps.setInt(i, xo.getInt(name));
                break;
            case "BIGINT":
                ps.setLong(i, xo.getLong(name));
                break;
            case "REAL":
                ps.setFloat(i, xo.getFloat(name));
                break;
            case "TEXT":
                ps.setString(i, xo.getString(name));
                break;
            default:
                ps.setString(i, xo.getString(name));
                break;
        }
    }


    private String repeat(String s, int n) {
        if (s == null) {
            return null;
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

}
