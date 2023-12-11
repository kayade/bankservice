package bankservice;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class DBManager {

    private static volatile DBManager dB = null;
    private String dbName = "msdb";
    private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    private String protocol = "jdbc:derby:";
    private Connection con = null;
    private String busyTable = "|";

    private DBManager() {

        //Prevent form the reflection api.
        if (dB != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }

        jdbconnector();
    }

    public static DBManager getDBManager() {

        //Double check locking pattern
        if (dB == null) { //Check for the first time

            synchronized (DBManager.class) {   //Check for the second time.

                //if there is no instance available... create new one
                if (dB == null) {

                    dB = new DBManager();
                }
            }
        }

        return dB;
    }

    private static void turnOnBuiltInUsers(Connection conn) throws
            SQLException {

        System.out.println("Turning on authentication.");
        Statement s = conn.createStatement();
        // Setting and Confirming requireAuthentication
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.connection.requireAuthentication', 'true')");
        ResultSet rs = s.executeQuery(
                "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY("
                + "'derby.connection.requireAuthentication')");
        rs.next();
        System.out.println("Value of requireAuthentication is "
                + rs.getString(1));
        // Setting authentication scheme to Derby
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.authentication.provider', 'BUILTIN')");
        // Creating some sample users
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.user.bizdOy', 'rM>@XvA*LW')");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.user.Demo', 'Demo')");
        // Setting default connection mode to no access
        // (user authorization)
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.database.defaultConnectionMode', 'noAccess')");
        // Confirming default connection mode
        rs = s.executeQuery(
                "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY("
                + "'derby.database.defaultConnectionMode')");

        rs.next();
        System.out.println("Value of defaultConnectionMode is "
                + rs.getString(1));
        // Defining read-write users
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.database.fullAccessUsers', 'bizdOy')");
        // Confirming full-access users
        rs = s.executeQuery(
                "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY("
                + "'derby.database.fullAccessUsers')");
        rs.next();
        System.out.println("Value of fullAccessUsers is "
                + rs.getString(1));
        // Defining read-only users
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.database.readOnlyAccessUsers', 'Demo')");
        // Confirming read-only-access users
        rs = s.executeQuery(
                "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY("
                + "'derby.database.readOnlyAccessUsers')");
        rs.next();
        System.out.println("Value of readOnlyAccessUsers is "
                + rs.getString(1));
        // We would set the following property to TRUE only
        // when we were ready to deploy.
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                + "'derby.database.propertiesOnly', 'true')");
        s.close();
    }

    private void jdbconnector() {

        try {

            Statement stmt = null;

            // load  the appropriate JDBC Driver
            Class.forName(driver).newInstance();
            System.out.println("Loaded the appropriate driver");

            // get a client connection using DriverManager
            Properties p = new Properties();
            p.put("user", "bizdOy");
            p.put("password", "rM>@XvA*LW");

            String folderLocation = (dbName);

            File file1 = new File(folderLocation);

            if (file1.exists()) {

                // Get database connection  via DriverManager api
                con = DriverManager.getConnection(protocol + dbName + ";bootPassword=gkpl11o1d7;", p);

                System.out.println("DBManager:Got a client connection via the DriverManager.");

                stmt = con.createStatement();

            } else {

                System.out.println("DBManager:Folder Does Not Exists.......");

                // Get database connection via DriverManager api
                con = DriverManager.getConnection(protocol + dbName
                        + ";create=true;dataEncryption=true;bootPassword=gkpl11o1d7;", p);

                System.out.println("DBManager:Got a client connection via the DriverManager.");

                turnOnBuiltInUsers(con);

                System.out.println("DBManager:Got an embedded connection.");

                stmt = con.createStatement();

                this.createSTables();

                System.out.println("DBManager:YEP!!!. About to");
                this.initialQueries();

                System.out.println("DBManager:YEP!!!. Done");
            }

        } catch (Exception sqle) {

            System.out.println("DBManager:Failure making connection: " + sqle);

            try {

                Thread.sleep(1000);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }

            jdbconnector();

            sqle.printStackTrace();
        }
    }

    private void createSTables() {

        Statement stmt = null;

        try {

            stmt = con.createStatement();

            stmt.executeUpdate(
                    "CREATE TABLE api_clients(id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,"
                    + " status VARCHAR(25), create_at VARCHAR(255),"
                    + " modified_at VARCHAR(255), name VARCHAR(255),"
                    + " secret_key VARCHAR(255),"
                    + " allowable_ips VARCHAR(255), encryption_iv VARCHAR(255),"
                    + " encryption_password VARCHAR(255), encryption_salt VARCHAR(255),"
                    + " username VARCHAR(255), password VARCHAR(255),"
                    + " hash_key VARCHAR(255), allowable_requests VARCHAR(255), environment VARCHAR(25))");

            stmt.executeUpdate(
                    "CREATE TABLE customer_info(id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,"
                    + " status VARCHAR(25), create_at VARCHAR(255),"
                    + " modified_at VARCHAR(255), gender VARCHAR(255),"
                    + " first_name VARCHAR(255), middle_name VARCHAR(255),"
                    + " surname VARCHAR(255), date_of_birth VARCHAR(255),"
                    + " nationality VARCHAR(255), national_identity_number VARCHAR(255),"
                    + " residential_address VARCHAR(255), residential_state VARCHAR(255),"
                    + " local_government VARCHAR(255), state_of_origin VARCHAR(255),"
                    + " mobile_number VARCHAR(255), email VARCHAR(255), marital_status VARCHAR(25),"
                    + " bvn VARCHAR(25))");

            stmt.executeUpdate(
                    "CREATE TABLE customer_accounts(id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,"
                    + " status VARCHAR(25), create_at VARCHAR(255),"
                    + " modified_at VARCHAR(255), customer_info_id VARCHAR(255),"
                    + " account_type VARCHAR(255), account_number VARCHAR(255),"
                    + " account_available_balance VARCHAR(255), account_balance VARCHAR(255))");

            stmt.executeUpdate(
                    "CREATE TABLE transfer_transactions(id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,"
                    + " status VARCHAR(25), create_at VARCHAR(255),"
                    + " modified_at VARCHAR(255), narration VARCHAR(255),"
                    + " transaction_reference VARCHAR(255), transaction_amount VARCHAR(255),"
                    + " fee_amount VARCHAR(255), currency VARCHAR(255),"
                    + " customer_id VARCHAR(255), destination_customer_number VARCHAR(255),"
                    + " status_reason VARCHAR(255))");

            stmt.executeUpdate(
                    "CREATE TABLE withdraw_transactions(id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,"
                    + " status VARCHAR(25), create_at VARCHAR(255),"
                    + " modified_at VARCHAR(255), transaction_reference VARCHAR(255),"
                    + " transaction_amount VARCHAR(255), fee_amount VARCHAR(255),"
                    + " customer_id VARCHAR(255), status_reason VARCHAR(255))");

            stmt.executeUpdate(
                    "CREATE TABLE deposit_transactions(id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,"
                    + " status VARCHAR(25), create_at VARCHAR(255),"
                    + " modified_at VARCHAR(255), transaction_reference VARCHAR(255),"
                    + " transaction_amount VARCHAR(255), customer_id VARCHAR(255),"
                    + " status_reason VARCHAR(255))");

        } catch (Exception sqle) {
            System.out.println(
                    "DBManager:SQLException when querying on the database connection; " + sqle);
        } finally {

            if (stmt != null) {

                try {

                    stmt.close();
                } catch (Exception ex) {

                    System.out.println("DBManager:Closing statements; " + ex);
                }
            }
        }
    }

    private int countString(String toExtract, String delim) {

        int ct = 0;
        int dP = 0, cStart = 0, pDp = -1, dif = 0;

        dP = toExtract.indexOf(delim, cStart);

        while (dP != -1) {

            dif = dP - pDp;

            if (dif > 1) {

                ct++;

            }

            cStart = dP + 1;
            pDp = dP;

            dP = toExtract.indexOf(delim, cStart);
        }

        if (ct > 0) {

            if (pDp < toExtract.length() - 1) {

                ct++;
            }

        }

        return ct;
    }

    private String extractString(String toExtract, String delim, int tokenCount) {

        String eString = "";
        int dP = 0, pDp = -1, cStart = 0, pStart = 0, dif = 0;

        for (int a = 0; a < tokenCount; a++) {

            dP = toExtract.indexOf(delim, cStart);

            if (dP != -1) {

                dif = dP - pDp;

                if (dif < 2) {

                    a--;

                }

                pStart = cStart;
                cStart = dP + 1;
                pDp = dP;

            } else {

                if (a > 0) {

                    if (a == tokenCount - 1) {

                        pStart = cStart;
                        dP = toExtract.length();
                    }

                } else {

                    dP = -1;
                }

                break;
            }
        }

        if (dP != -1) {

            eString = toExtract.substring(pStart, dP);
        }

        return eString;
    }

    public String insertRecord(String tName, String record) {

        isDBConnected();

        /**
         * # to select specific columns INSERT INTO users (firstname, lastname)
         * VALUES ('Joe', 'Cool') RETURNING id, firstname; # to return every
         * column INSERT INTO users (firstname, lastname) VALUES ('Joe', 'Cool')
         * RETURNING *;
         */
        if (this.busyTable.contains("|" + tName + "|")) {

            try {

                Thread.sleep(1000);

                insertRecord(tName, record);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }
        }

        Statement stmt = null;
        ResultSet rs = null;

        String rec = "", sm = "";

        this.busyTable = tName + "|";

//            System.out.println("DBManager:insertRecord:tName:" + tName);
        String pQuery = "INSERT INTO " + tName + " VALUES (DEFAULT, '";

        int pC = countString(record, "Ð");

//            System.out.println("DBManager:insertRecord:pC:" + pC);
        for (int d = 0; d < pC; d++) {

            if (d != (pC - 1)) {

                pQuery += extractString(record, "Ð", d + 1) + "', '";

            } else {

                pQuery += extractString(record, "Ð", d + 1) + "')";
            }
        }

        System.out.println("DBManager:insertRecord:pQuery:" + pQuery);

        try {

            stmt = con.createStatement();
//            stmt.execute(pQuery);
            stmt.executeUpdate(pQuery, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();

            if (rs.next()) {

                rec = rs.getString(1); //this is the auto-generated key for your use
            }

            stmt.close();

        } catch (Exception ex) {

            System.out.println("Exception: DBManager: insertRecord: " + ex);
        }

        busyTable = busyTable.replace("|" + tName + "|", "|");

        return rec;
    }

    public int countTable(String tName, String sWhere) {

        isDBConnected();

        if (busyTable.contains("|" + tName + "|")) {

            try {

                Thread.sleep(1000);

                countTable(tName, sWhere);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }
        }

        Statement stmt = null;
        ResultSet rs = null;

        int cot = 0;

        try {
            stmt = con.createStatement();

            stmt.execute("select COUNT(*) from " + tName + sWhere);

            rs = stmt.getResultSet();

            if (rs != null) {
                while (rs.next()) {

                    cot = Integer.parseInt(rs.getString(1));
                }
            }

            stmt.close();
        } catch (Exception ex) {

            System.out.println("Exception: DBManager: countTable: " + ex);
        }

        return cot;
    }

    public void retrieveRecords(CopyOnWriteArrayList<String> lB, String selectFields, String tName, String sWhere) {

        isDBConnected();

        if (busyTable.contains("|" + tName + "|")) {

            try {

                Thread.sleep(1000);

                retrieveRecords(lB, selectFields, tName, sWhere);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }
        }

        Statement stmt = null;
        ResultSet rs = null;

        String rLog = "";

        try {

            stmt = con.createStatement();

            stmt.execute("select " + selectFields + " from " + tName + sWhere);

            rs = stmt.getResultSet();

            if (rs != null) {

                ResultSetMetaData columns = rs.getMetaData();

                int cCot = 0;

                cCot = columns.getColumnCount();

                while (rs.next()) {

                    for (int d = 1; d <= cCot; d++) {

                        rLog += rs.getString(d) + "Ð";
                    }

                    lB.add(rLog);
                    rLog = "";
                }
            }

            stmt.close();

        } catch (Exception ex) {

            System.out.println("Exception: 1: DBManager: retrieveRecords: " + ex);
        }
    }

    public void updateRecord(String tName, String fSet, String vSet, String fWhere, String vWhere, String extra) {

        isDBConnected();

        if (this.busyTable.contains("|" + tName + "|")) {

            try {

                Thread.sleep(1000);

                updateRecord(tName, fSet, vSet, fWhere, vWhere, extra);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }
        }

        Statement stmt = null;

        this.busyTable = tName + "|";

        String uQuery = "update " + tName + " set " + fSet + " = '"
                + vSet + "' where " + fWhere + " =" + "'"
                + vWhere + "'" + "" + extra;

        try {

            stmt = con.createStatement();
            stmt.executeUpdate(uQuery);
            stmt.close();

        } catch (Exception ex) {

            System.out.println("Exception: 1: DBManager: updateRecord: " + ex);
        }

        this.busyTable = this.busyTable.replace("|" + tName + "|", "|");
    }

    public void deleteRecord(String tName, String fWhere, String vWhere) {

        isDBConnected();

        if (this.busyTable.contains("|" + tName + "|")) {

            try {

                Thread.sleep(1000);

                deleteRecord(tName, fWhere, vWhere);

            } catch (InterruptedException e) {

                e.printStackTrace();

            }
        }

        Statement stmt = null;

        String wClause = " WHERE " + fWhere + " = '" + vWhere + "'";

        int tC = this.countTable(tName, wClause);

        if (tC > 0) {

            busyTable = tName + "|";

            String dQuery = "DELETE FROM " + tName + " WHERE " + fWhere + " = '" + vWhere + "'";

            try {

                stmt = con.createStatement();
                stmt.executeUpdate(dQuery);
                stmt.close();

            } catch (Exception ex) {

                System.out.println("Exception: 1: DBManager: deleteRecord: " + ex);
            }

            busyTable = busyTable.replace("|" + tName + "|", "|");
        }

    }

    private void initialQueries() {

        String attrib = "";

        String tim = LocalDateTime.now().toString();

        attrib = "ACTIVE" + "Ð" + tim + "Ð" + tim + "Ð" + "ipNX" + "Ð" + "Lll8wEc2NPdZO4sBnM"
                + "Ð" + "ALL" + "Ð" + "0a77464fcf0ca7b751e42897b04dc212"
                + "Ð" + "DBB279F255FB3973F32A7EF1" + "Ð" + "6c6f7347b77bb4e571d7f21b5ce64c0e"
                + "Ð" + "ipNXAdmin" + "Ð" + "npg0rIVjG$HlsnbB"
                + "Ð" + "MOzz+$&U~4N~Yed!" + "Ð" + " " + "Ð" + "TEST";

        String id = this.insertRecord("api_clients", attrib);

        System.out.println("id:" + id);

        attrib = "ACTIVE" + "Ð" + tim + "Ð" + tim + "Ð" + "ipNX" + "Ð" + "5OlJeRyliagzEdpJnp"
                + "Ð" + "ALL" + "Ð" + "f1e158df546837c2a12ab41c047aa7a9"
                + "Ð" + "1C574F1C70E3FD69ABC7FF4F" + "Ð" + "d15607f208b141f47dbe8cf514290468"
                + "Ð" + "ipNXAdmin" + "Ð" + "^mEK*G808swmj4zs"
                + "Ð" + "sjhjCblhzH@cpu^G" + "Ð" + " " + "Ð" + "LIVE";

        this.insertRecord("api_clients", attrib);
    }

    private void shutdown() {

        try {
            // the shutdown=true attribute shuts down Derby
            DriverManager.getConnection("jdbc:derby:;shutdown=true");

            // To shut down a specific database only, but keeep the
            // engine running (for example for connecting to other
            // databases), specify a database in the connection URL:
            //DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
        } catch (SQLException se) {
            if (((se.getErrorCode() == 50000)
                    && ("XJ015".equals(se.getSQLState())))) {
                // we got the expected exception
                System.out.println("Derby shut down normally");
                // Note that for single database shutdown, the expected
                // SQL state is "08006", and the error code is 45000.
            } else {
                // if the error code or SQLState is different, we have
                // an unexpected exception (shutdown failed)
                System.err.println("Derby did not shut down normally");
                se.printStackTrace();
            }
        }
    }

    public void isDBConnected() {

        try {

            if (con == null || con.isClosed()) {

                jdbconnector();
            }

        } catch (SQLException ignored) {

            System.out.println("Exception:DBManager:isDbConnected:ignored:" + ignored);

        } catch (Exception ex) {

            System.out.println("Exception:DBManager:isDbConnected:ex:" + ex);
        }
    }

}
