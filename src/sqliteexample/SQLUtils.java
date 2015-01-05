package sqliteexample;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * SVN information
 * $Revision: 2971 $
 * $Author: ftrujillo $
 * $Date: 2014-12-31 11:50:45 -0700 (Wed, 31 Dec 2014) $
 * $HeadURL: http://svn/NSG/comp_ssd/software/trunk/NetBeansProjects/Examples/SQLiteExample/src/sqliteexample/SQLUtils.java $
 *
 */
public class SQLUtils {

    private static boolean debug = false;

    public SQLUtils() {
    }

    public static void setDebug() {
        SQLUtils.debug = true;
    }

    public static void clrDebug() {
        SQLUtils.debug = false;
    }

    /*
     * <pre>
     * To create a table, we give a name to a table and to its columns. Each column can have one of these data types:
     * NULL    The value is a NULL value
     * INTEGER a signed integer
     * REAL    a floating point value
     * TEXT    a text string
     * BLOB    a blob of data
     *
     * Any fields such as VARCHAR(255) will be tranformed into 1 of 5 above    https://www.sqlite.org/datatype3.html
     *
     * </pre
     */
    public static boolean doesTableExist(Connection connection, String tableName) throws SQLException {
        boolean tableExists = false;
        String[] types = {
            "TABLE",
            "VIEW"
        };

        ResultSet rs = connection.getMetaData().getTables(null, null, tableName, types);
        if (rs.next()) {
            tableExists = true;
        } else {
            tableExists = false;
        }
        return (tableExists);
    }

    public static List<String> getResultsAsListOfCSV(ResultSet rs) throws SQLException {
        List<String> csvResultsList = new ArrayList<String>();

        List<String> columnNamesList = SQLUtils.getColumnNames(rs);
        StringBuilder sb = new StringBuilder();
        Iterator<String> itr = columnNamesList.listIterator();
        while (itr.hasNext()) {
            String columnName = itr.next();
            sb.append(columnName).append(",");
        }
        csvResultsList.add(sb.toString().replaceAll("([,]$)", ""));
        sb.setLength(0); // clear
        while (rs.next()) {
            itr = columnNamesList.listIterator();
            while (itr.hasNext()) {
                String columnName = itr.next();
                String value = rs.getString(columnName);
                if (value == null) {
                    value = new String("");
                } else {
                    value = value.replaceAll(",", "_COMMA_");
                }
                sb.append(value).append(",");
            }
            csvResultsList.add(sb.toString().replaceAll("([,]$)", ""));
            sb.setLength(0); // clear
        }

        return (csvResultsList);
    }

    public static List<String> getColumnNames(ResultSet rs) throws SQLException {
        ArrayList<String> columnNamesList = new ArrayList<String>();
        int columnCount = rs.getMetaData().getColumnCount();
        for (int colNo = 1; colNo <= columnCount; colNo++) {
            String columnName = rs.getMetaData().getColumnName(colNo);
            columnNamesList.add(columnName);
        }
        return columnNamesList;
    }
    
    public static List<String> getTableSchemaCSV(Connection connection, String tableName) throws SQLException {
        List<String> resultsList = null;

        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            String dbProductName = connection.getMetaData().getDatabaseProductName();

            String sql = null;
            if (dbProductName.trim().toUpperCase().matches(".*SQLITE.*")) {
                sql = String.format("PRAGMA table_info(%s);", tableName);
            } else if (dbProductName.trim().toUpperCase().matches(".*MICROSOFT SQL SERVER.*")) {
                sql = String.format("Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_NAME = '%s'", tableName);
            } else {
                String msg = "ADMIN => Please update getTableSchemaCSV() for dbProductName => " + dbProductName.trim().toUpperCase();
                throw new SQLUtilsException(msg);
            }

            ResultSet rs = statement.executeQuery(sql);
            resultsList = SQLUtils.getResultsAsListOfCSV(rs);
        } catch (SQLUtilsException ex) {
            Logger.getLogger(SQLUtils.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        return (resultsList);
    }
    
    public static List<String> getTablesCSV(Connection connection) throws SQLException {
        List<String> resultsList = null;

        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            String dbProductName = connection.getMetaData().getDatabaseProductName();

            String sql = null;
            if (dbProductName.trim().toUpperCase().matches(".*SQLITE.*")) {
                sql = String.format("SELECT type,name FROM sqlite_master WHERE type='table';");
            } 
//            else if (dbProductName.trim().toUpperCase().matches(".*MICROSOFT SQL SERVER.*")) {
//                sql = String.format("Select * From INFORMATION_SCHEMA.COLUMNS Where TABLE_NAME = '%s'", tableName);
//            } 
            else {
                String msg = "ADMIN => Please update getTablesCSV() for dbProductName => " + dbProductName.trim().toUpperCase();
                throw new SQLUtilsException(msg);
            }

            ResultSet rs = statement.executeQuery(sql);
            resultsList = SQLUtils.getResultsAsListOfCSV(rs);
        } catch (SQLUtilsException ex) {
            Logger.getLogger(SQLUtils.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-1);
        }

        return (resultsList);
    }
    
//

    public static void displayTableSchemaCSV(Connection connection, String tableName) throws SQLException {
        List<String> resultsList = SQLUtils.getTableSchemaCSV(connection, tableName);
        SQLUtils.displayList(resultsList);
    }
    
    public static void displayTablesCSV(Connection connection) throws SQLException {
        List<String> resultsList = SQLUtils.getTablesCSV(connection);
        SQLUtils.displayList(resultsList);
    }
    
    public static void displayTableSchema(Connection connection, String tableName) throws SQLException {
        List<String> resultsList = SQLUtils.getTableSchemaCSV(connection, tableName);
        SQLUtils.displayFormattedList(resultsList);
    }

    public static void displayList(List<String> myList) {
        Iterator<String> itr = myList.listIterator();
        while (itr.hasNext()) {
            String line = itr.next();
            System.out.println(line);
        }
        System.out.println("");
    }

    public static void displayResultSetCSV(ResultSet rs) throws SQLException {
        List<String> csvResultsList = SQLUtils.getResultsAsListOfCSV(rs);
        SQLUtils.displayList(csvResultsList);
    }

    public static void displayResultSet(ResultSet rs) throws SQLException {
        List<String> csvResultsList = SQLUtils.getResultsAsListOfCSV(rs);
        SQLUtils.displayFormattedList(csvResultsList);
    }


    /**
     * http://www.xyzws.com/javafaq/how-to-retrieve-multiple-result-sets-from-a-stored-procedure-in-jdbc/172
     *
     * @param connection
     * @param sql
     * @return List &lt;ArrayList&lt;String&gt;&gt;
     * @throws java.sql.SQLException
     */
    public static List<List<String>> executeProcedureWithMultipleResultSets(Connection connection, String sql) throws SQLException {
        List<List<String>> resultsList = new ArrayList<List<String>>();

        try (CallableStatement stmt = connection.prepareCall(sql)) {
            boolean results = stmt.execute();

            //Loop through the available result sets.
            while (results) {
                try (ResultSet rs = stmt.getResultSet()) {
                    List<String> tmpList = SQLUtils.getResultsAsListOfCSV(rs);
                    if (SQLUtils.debug) {
                        SQLUtils.displayList(tmpList);
                    }
                    resultsList.add(tmpList);
                }
                //Check for next result set
                results = stmt.getMoreResults();
                if (SQLUtils.debug) {
                    System.out.println("\n===================================================\n");
                }
            }
        }
        return (resultsList);
    }

    public static void displayFormattedList(List<String> myList) {
        Map<Integer, Integer> columnWidths = new LinkedHashMap<Integer, Integer>();
        Map<Integer, String> formatStrings = new LinkedHashMap<Integer, String>();
        int numColumns = 0;

        for (int ii = 0; ii < myList.size(); ii++) {
            String[] awkedLine = myList.get(ii).split("[,]");
            numColumns = awkedLine.length;

            for (int jj = 0; jj < numColumns; jj++) {
                int width = awkedLine[jj].length();
                if (columnWidths.containsKey(jj) == false) {
                    columnWidths.put(jj, width);
                } else {
                    if (width > columnWidths.get(jj)) {
                        columnWidths.put(jj, width);
                    }
                }
            }
        }

        for (int columnNo = 0; columnNo < numColumns; columnNo++) {
            formatStrings.put(columnNo, String.format("%s%ss | ", "%-", columnWidths.get(columnNo)));
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> itr = myList.listIterator();
        while (itr.hasNext()) {
            String[] awkedLine = itr.next().split("[,]");
            sb.setLength(0); // clear

            for (int columnNo = 0; columnNo < awkedLine.length; columnNo++) {
                String columnValue = awkedLine[columnNo];
                if (columnValue.isEmpty()) {
                    sb.append(String.format(formatStrings.get(columnNo), "null"));
                } else {
                    columnValue = columnValue.replaceAll("_COMMA_", ",").replaceAll("\\|", "_PIPE_");
                    sb.append(String.format(formatStrings.get(columnNo), columnValue));
                }
            }
            System.out.println(sb.toString().replaceAll("\\|\\s+$", ""));
        }
        System.out.println("");
    }

}
