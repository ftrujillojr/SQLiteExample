package sqliteexample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This example produces this output. It is just a start to show a SCHEMA DUMP,
 * SELECT, INSERT, UPDATE, and DELETE.
 * 
 * Jar files for sqllite JDBC are in ./lib directory.
 *
 * <pre>
 * cid,name,type,notnull,dflt_value,pk
 * 0,Id,INTEGER,0,,1
 * 1,LastName,TEXT,1,,0
 * 2,FirstName,TEXT,1,,0
 * 3,Age,INTEGER,1,,0
 * 4,Email,TEXT,0,,0
 *
 * cid | name      | type    | notnull | dflt_value | pk
 * 0   | Id        | INTEGER | 0       | null       | 1
 * 1   | LastName  | TEXT    | 1       | null       | 0
 * 2   | FirstName | TEXT    | 1       | null       | 0
 * 3   | Age       | INTEGER | 1       | null       | 0
 * 4   | Email     | TEXT    | 0       | null       | 0
 *
 *
 * Id,LastName,FirstName,Age,Email
 * 1,Dinosaur,Barney,10000,barney@prehistoric.org
 * 2,Coyote,Wiley,50,wiley@acme.org
 * 3,Squarepants,Spongebob,15,spongebob@pineapple_grove.org
 *
 * Id | LastName    | FirstName | Age   | Email
 * 1  | Dinosaur    | Barney    | 10000 | barney@prehistoric.org
 * 2  | Coyote      | Wiley     | 50    | wiley@acme.org
 * 3  | Squarepants | Spongebob | 15    | spongebob@pineapple_grove.org
 *
 * </pre>
 *
 * @author ftrujillo
 */
public class SQLiteExample {

    public static void main(String[] args) {
        try {

            // Class.forName() is not directly related to JDBC at all. It simply loads a class and throws exception when not loaded.
            // In other words, a missing library...
            Class.forName("org.sqlite.JDBC");
            // There is NO concept of USERNAME and PASSWORDS in SQLite.  Stop looking.
            // There are NO backwards CURSORS.  Once you read a result set, it is closed.
            String connectionUrl = "jdbc:sqlite:test.db";
            Connection connection = DriverManager.getConnection(connectionUrl);

// There is NO USE statement in SQLite.
// ====================================================================            
// ATTACH DATABASE 'very_long_name_of_my_external_db_file.sqlite' AS v;
// SELECT * FROM v.table;

            //SQLUtils.setDebug();
            if (SQLUtils.doesTableExist(connection, "Person") == false) {
                String createSql = "CREATE TABLE Person(\n"
                        + "Id          INTEGER              PRIMARY KEY  AUTOINCREMENT,\n"
                        + "LastName    TEXT                 NOT NULL,\n"
                        + "FirstName   TEXT                 NOT NULL,\n"
                        + "Age         INTEGER              NOT NULL,\n"
                        + "Email       TEXT                 NULL\n"
                        + ");\n";
                try (Statement statement = connection.createStatement()) {
                    statement.setQueryTimeout(30);
                    System.out.println(createSql);
                    statement.executeUpdate(createSql);
                }
            }

            String selectSql = "SELECT Id FROM Person WHERE LastName = ? AND FirstName = ?";

            String insertSql = "INSERT INTO Person (LastName, FirstName, Age, Email) "
                    + "VALUES (?, ?, ?, ?)";

            String updateSql = "UPDATE Person "
                    + "SET LastName = ?, FirstName = ?, Age = ?, Email = ?"
                    + "WHERE (Id = ?);";

            String deleteSql = "DELETE FROM Person WHERE LastName = ? AND FirstName = ?";

            String[][] sampleData = {
                {"Dinosaur", "Barney", "10000", "barney@prehistoric.org"},
                {"Coyote", "Wiley", "50", "wiley@acme.org"},
                {"Bunny", "Bugs", "50", "bugs@warner_bros.org"},
                {"Squarepants", "Spongebob", "15", "spongebob@pineapple_grove.org"}
            };

            for (int ii = 0; ii < sampleData.length; ii++) {
                String[] vals = sampleData[ii];

                try (PreparedStatement psSelect = connection.prepareStatement(selectSql)) {
                    psSelect.setQueryTimeout(30);
                    psSelect.setString(1, vals[0]);
                    psSelect.setString(2, vals[1]);
                    ResultSet rs = psSelect.executeQuery();
                    int id = -1;
                    while (rs.next()) {
                        id = rs.getInt("Id");
                    }
                    if (id == -1) { // INSERT
                        try (PreparedStatement psInsert = connection.prepareStatement(insertSql)) {
                            psInsert.setQueryTimeout(30);
                            psInsert.setString(1, vals[0]);
                            psInsert.setString(2, vals[1]);
                            psInsert.setInt(3, Integer.parseInt(vals[2]));
                            psInsert.setString(4, vals[3]);
                            psInsert.executeUpdate();
                        }
                    } else { // UPDATE
                        try (PreparedStatement psUpdate = connection.prepareStatement(updateSql)) {
                            psUpdate.setQueryTimeout(30);
                            psUpdate.setString(1, vals[0]);
                            psUpdate.setString(2, vals[1]);
                            psUpdate.setInt(3, Integer.parseInt(vals[2]));
                            psUpdate.setString(4, vals[3]);
                            psUpdate.setInt(5, id);
                            psUpdate.executeUpdate();
                        }
                    }
                }
            }

            try (PreparedStatement psSelect = connection.prepareStatement(selectSql)) {
                psSelect.setQueryTimeout(30);
                psSelect.setString(1, "Bunny");
                psSelect.setString(2, "Bugs");
                ResultSet rs = psSelect.executeQuery();
                int id = -1;
                while (rs.next()) {
                    id = rs.getInt("Id");
                }
                if (id != -1) {  // If it exists, then delete.
                    try (PreparedStatement psDelete = connection.prepareStatement(deleteSql)) {
                        psDelete.setQueryTimeout(30);
                        psDelete.setString(1, "Bunny");
                        psDelete.setString(2, "Bugs");
                        psDelete.executeUpdate();
                    }
                }
            }

            List<String> tables = SQLUtils.getTablesCSV(connection);
            SQLUtils.displayList(tables);
            
            
            List<String> tableSchema = SQLUtils.getTableSchemaCSV(connection, "Person");
            SQLUtils.displayList(tableSchema);
            SQLUtils.displayFormattedList(tableSchema);

            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(30);
                String sql = "SELECT * from Person;";
                ResultSet rs = statement.executeQuery(sql);
                List<String> resultsList = SQLUtils.getResultsAsListOfCSV(rs);
                SQLUtils.displayList(resultsList);
                SQLUtils.displayFormattedList(resultsList);
            }

            System.exit(0);
        } // main
        catch (ClassNotFoundException ex) {
            Logger.getLogger(SQLiteExample.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(SQLiteExample.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

} // class SQLiteExample

//          Helper helper = new Helper();
//          
//          ArgParseImpl ap = new ArgParseImpl();  // ArgParseExceptions and RegExExeceptions handled by this impl.
//          
//            ap.add_REQSwitch_REQArg("-i", "--input",  "Input filename",   "^[a-zA-Z0-9:\\.\\/\\_]+$", "");
//            ap.add_REQSwitch_REQArg("-o", "--output", "Output filename",  "^[a-zA-Z0-9:\\.\\/\\_]+$", "");
//            ap.add_OPTSwitch_REQArg("-d", "--debug",  "Sets DEBUG level", "^[0-9]+$",                "0");
//            ap.add_OPTSwitch_OPTArg("-h", "--help",   "Display Usage",    "",                        "0");
//            
//            ap.addExample("java -jar ./dist/SQLiteExample.jar -i input.txt -o output.txt\n");
//            ap.addExample("java -jar ./dist/SQLiteExample.jar -i input.txt -o output.txt --debug 3\n");
//          
//          HashMap<String, String> options = ap.parse(args);
