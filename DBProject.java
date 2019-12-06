/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */
 
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
 
 
/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */
public class DBProject {
 
    // reference to physical database connection.
    private Connection _connection = null;
 
    // handling the keyboard inputs through a BufferedReader
    // This variable can be global for convenience.
    static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
 
    /**
     * Creates a new instance of DBProject
     *
     * @param hostname the MySQL or PostgreSQL server hostname
     * @param database the name of the database
     * @param username the user name used to login to the database
     * @param password the user login password
     * @throws java.sql.SQLException when failed to make a connection.
     */
    public DBProject(String dbname, String dbport, String user, String passwd) throws SQLException {
 
        System.out.print("Connecting to database...");
        try {
            // constructs the connection URL
            String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
            System.out.println("Connection URL: " + url + "\n");
 
            // obtain a physical connection
            this._connection = DriverManager.getConnection(url, user, passwd);
            System.out.println("Done");
        } catch (Exception e) {
            System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
            System.out.println("Make sure you started postgres on this machine");
            System.exit(-1);
        } // end catch
    }// end DBProject
 
    /**
     * Method to execute an update SQL statement. Update SQL instructions includes
     * CREATE, INSERT, UPDATE, DELETE, and DROP.
     *
     * @param sql the input SQL string
     * @throws java.sql.SQLException when update failed
     */
    public void executeUpdate(String sql) throws SQLException {
        // creates a statement object
        Statement stmt = this._connection.createStatement();
 
        // issues the update instruction
        stmt.executeUpdate(sql);
 
        // close the instruction
        stmt.close();
    }// end executeUpdate
 
    /**
     * Method to execute an input query SQL instruction (i.e. SELECT). This method
     * issues the query to the DBMS and outputs the results to standard out.
     *
     * @param query the input query string
     * @return the number of rows returned
     * @throws java.sql.SQLException when failed to execute the query
     */
    public int executeQuery(String query) throws SQLException {
        // creates a statement object
        Statement stmt = this._connection.createStatement();
 
        // issues the query instruction
        ResultSet rs = stmt.executeQuery(query);
 
        /*
         ** obtains the metadata object for the returned result set. The metadata
         ** contains row and column info.
         */
        ResultSetMetaData rsmd = rs.getMetaData();
        int numCol = rsmd.getColumnCount();
        int rowCount = 0;
 
        // iterates through the result set and output them to standard out.
        boolean outputHeader = true;
        while (rs.next()) {
            if (outputHeader) {
                for (int i = 1; i <= numCol; i++) {
                    System.out.print(rsmd.getColumnName(i) + "\t");
                }
                System.out.println();
                outputHeader = false;
            }
            for (int i = 1; i <= numCol; ++i)
                System.out.print(rs.getString(i) + "\t");
            System.out.println();
            ++rowCount;
        } // end while
        stmt.close();
        return rowCount;
    }// end executeQuery
 
    /**
     * Method to close the physical connection if it is open.
     */
    public void cleanup() {
        try {
            if (this._connection != null) {
                this._connection.close();
            } // end if
        } catch (SQLException e) {
            // ignored.
        } // end try
    }// end cleanup
 
    /**
     * The main execution method
     *
     * @param args the command line arguments this inclues the <mysql|pgsql> <login
     *             file>
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: " + "java [-classpath <classpath>] " + DBProject.class.getName()
                    + " <dbname> <port> <user>");
            return;
        } // end if
 
        Greeting();
        DBProject esql = null;
        try {
            // use postgres JDBC driver.
            Class.forName ("org.postgresql.Driver");
            // instantiate the DBProject object and creates a physical
            // connection.
            String dbname = args[0];
            String dbport = args[1];
            String user = args[2];
            esql = new DBProject(dbname, dbport, user, "12345");
 
            boolean keepon = true;
            while (keepon) {
                // These are sample SQL statements
                System.out.println("MAIN MENU");
                System.out.println("---------");
                System.out.println("1. Add new customer");
                System.out.println("2. Add new room");
                System.out.println("3. Add new maintenance company");
                System.out.println("4. Add new repair");
                System.out.println("5. Add new Booking");
                System.out.println("6. Assign house cleaning staff to a room");
                System.out.println("7. Raise a repair request");
                System.out.println("8. Get number of available rooms");
                System.out.println("9. Get number of booked rooms");
                System.out.println("10. Get hotel bookings for a week");
                System.out.println("11. Get top k rooms with highest price for a date range");
                System.out.println("12. Get top k highest booking price for a customer");
                System.out.println("13. Get customer total cost occurred for a give date range");
                System.out.println("14. List the repairs made by maintenance company");
                System.out.println("15. Get top k maintenance companies based on repair count");
                System.out.println("16. Get number of repairs occurred per year for a given hotel room");
                System.out.println("17. < EXIT");
 
                switch (readChoice()) {
                case 1:
                    addCustomer(esql);
                    break;
                case 2:
                    addRoom(esql);
                    break;
                case 3:
                    addMaintenanceCompany(esql);
                    break;
                case 4:
                    addRepair(esql);
                    break;
                case 5:
                    bookRoom(esql);
                    break;
                case 6:
                    assignHouseCleaningToRoom(esql);
                    break;
                case 7:
                    repairRequest(esql);
                    break;
                case 8:
                    numberOfAvailableRooms(esql);
                    break;
                case 9:
                    numberOfBookedRooms(esql);
                    break;
                case 10:
                    listHotelRoomBookingsForAWeek(esql);
                    break;
                case 11:
                    topKHighestRoomPriceForADateRange(esql);
                    break;
                case 12:
                    topKHighestPriceBookingsForACustomer(esql);
                    break;
                case 13:
                    totalCostForCustomer(esql);
                    break;
                case 14:
                    listRepairsMade(esql);
                    break;
                case 15:
                    topKMaintenanceCompany(esql);
                    break;
                case 16:
                    numberOfRepairsForEachRoomPerYear(esql);
                    break;
                case 17:
                    keepon = false;
                    break;
                default:
                    System.out.println("Unrecognized choice!");
                    break;
                }// end switch
            } // end while
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            // make sure to cleanup the created table and close the connection.
            try {
                if (esql != null) {
                    System.out.print("Disconnecting from database...");
                    esql.cleanup();
                    System.out.println("Done\n\nBye !");
                } // end if
            } catch (Exception e) {
                // ignored.
            } // end try
        } // end try
    }// end main
 
    public static void Greeting() {
        System.out.println("\n\n*******************************************************\n"
                + "              User Interface                        \n"
                + "*******************************************************\n");
    }// end Greeting
 
    /*
     * Reads the users choice given from the keyboard
     *
     * @int
     **/
    public static int readChoice() {
        int input;
        // returns only if a correct value is given.
        do {
            System.out.print("Please make your choice: ");
            try { // read the integer, parse it and break.
                input = Integer.parseInt(in.readLine());
                break;
            } catch (Exception e) {
                System.out.println("Your input is invalid!");
                continue;
            } // end try
        } while (true);
        return input;
    }// end readChoice
 
    /*
     * Reads the users input from the keyboard
     *
     * @String
     **/
    public static String readText() {
        String input;
        
        do {
            try { // read line
                input = in.readLine();
                break;
            } catch (Exception e) {
                System.out.println("Your input is invalid!");
                continue;
            } // end try
        } while (true);
        return input;
    }// end readText
 
    public static int readInt() {
        int input;
        
        do {
            try { // read the integer, parse it and break.
                input = Integer.parseInt(in.readLine());
                break;
            } catch (Exception e) {
                System.out.println("Your input is invalid!");
                continue;
            } // end try
        } while (true);
        return input;
    }// end readInt
 
    public static double readDouble() {
        double input;
        
        do {
            try { // read the double, parse it and break.
                input = Double.parseDouble(in.readLine());
                break;
            } catch (Exception e) {
                System.out.println("Your input is invalid!");
                continue;
            } // end try
        } while (true);
        return input;
    }// end readInt
 
    public int getNextId(DBProject esql, String fieldName, String tableName) {
        String query = "Select Max(" + fieldName + ") from " + tableName;
        int next = 0;
 
        try {
            // creates a statement object
            Statement stmt = this._connection.createStatement();
 
            // issues the query instruction
            ResultSet rs = stmt.executeQuery(query);
 
            while (rs.next()) {
                next = rs.getInt(1) + 1;
            } // end while
 
            stmt.close();
        } catch (Exception e) {
            System.out.println(e);
        }
 
        return next;
    }
 
    public static void addCustomer(DBProject esql) {
        // Given customer details add the customer in the DB
        String name, lastName, address, dob, gender;
        int phone;
        System.out.print("Enter name: ");
        name = readText();
 
        System.out.print("Enter last name: ");
        lastName = readText();
 
        System.out.print("Enter Address: ");
        address = readText();
 
        System.out.print("Enter phone number: ");
        phone = readInt();
 
        System.out.print("Enter date of birth: ");
        dob = readText();
 
        System.out.print("Enter Gender: ");
        gender = readText();
 
        String sql = String.format(
                "INSERT INTO CUSTOMER(CUSTOMERID,FNAME,LNAME,ADDRESS,PHNO,DOB,GENDER)"
                        + " VALUES(%s,'%s','%s','%s',%s,'%s','%s') ",
                esql.getNextId(esql, "customerid", "customer"), name, lastName, address, phone, dob, gender);
 
        try {
            esql.executeUpdate(sql);
            System.out.println("The customer was succesfully added!");
        } catch (Exception e) {
            System.out.println("The customer couldn't be added.\nMake sure that "
                    + "name and last name are at most 30 characters long, phone number contains only numbers, date of birth"
                    + " is in the format MM/DD/YYYY and gender is Male, Female or Other.");
        }
 
    }// end addCustomer
 
    public static void addRoom(DBProject esql) {
        // Given room details add the room in the DB
        try {
            System.out.print("\tEnter hotelID: ");
            String hotelid = in.readLine();
 
            System.out.print("\tEnter room type: ");
            String roomType = in.readLine();
 
            String sql = String.format("INSERT INTO Room VALUES ( %s, %s, '%s')", hotelid,
                    esql.getNextId(esql, "roomno", "room"), roomType);
 
            esql.executeUpdate(sql);
            System.out.println("The room was succesfully added!");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end addRoom
 
    public static void addMaintenanceCompany(DBProject esql) {
        // Given maintenance Company details add the maintenance company in the DB
        String name, address, cert;
        boolean isCertified;
 
        System.out.print("Name of the Company: ");
        name = readText();
 
        System.out.print("Address: ");
        address = readText();
 
        System.out.print("Is it certified? (yes/no): ");
        cert = readText();
        while (!(cert.equalsIgnoreCase("yes") || cert.equalsIgnoreCase("no"))) {
            System.out.print("Wrong format.\nIs it certified? (yes/no):");
            cert = readText();
        }
        isCertified = true ? "yes".equalsIgnoreCase(cert) : false;
 
        String sql = String.format(
                "INSERT INTO MaintenanceCompany(cmpID, name, address,isCertified) VALUES(%s,'%s','%s',%s)",
                esql.getNextId(esql, "cmpID", "MaintenanceCompany"), name, address, isCertified);
 
        try {
            esql.executeUpdate(sql);
            System.out.println(name + " was succesfully added!");
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("The company couldn't be added.\nMake sure that "
                    + " the name is at most 30 characters and you provide a yes or no to is certified field.");
        }
 
    }// end addMaintenanceCompany
 
    public static void addRepair(DBProject esql) {
        // Given repair details add repair in the DB
        try {
 
            System.out.print("\tEnter hotelID: ");
            String hotelid = in.readLine();
 
            System.out.print("\tEnter room number: ");
            String roomNo = in.readLine();
 
            System.out.print("\tEnter maintenance company id: ");
            String compID = in.readLine();
 
            System.out.print("\tEnter repair date: ");
            String repDate = in.readLine();
 
            System.out.print("\tEnter repair description: ");
            String repairDescr = in.readLine();
 
            System.out.print("\tEnter repair type: ");
            String repairType = in.readLine();
 
            String sql = String.format("INSERT INTO Repair VALUES ( %s, %s, %s, %s, '%s', '%s', '%s')",
                    esql.getNextId(esql, "rID", "Repair"), hotelid, roomNo, compID, repDate, repairDescr, repairType);
 
            esql.executeUpdate(sql);
            System.out.println("The repair was succesfully added!");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end addRepair
 
    public static void bookRoom(DBProject esql) {
        // Given hotelID, roomNo and customer Name create a booking in the DB
        int hotelID, roomNo, numberOfPeople;
        double price;
        String date, fname, lname;
 
        System.out.print("Enter hotel ID: ");
        hotelID = readInt();
 
        System.out.print("Enter room number");
        roomNo = readInt();
 
        System.out.print("Enter Customer first name: ");
        fname = readText();
 
        System.out.print("Enter Customer last name: ");
        lname = readText();
 
        System.out.print("Enter booking date: ");
        date = readText();
 
        System.out.print("Enter number of people: ");
        numberOfPeople = readInt();
 
        System.out.print("Enter price: ");
        price = readDouble();
 
        String sql = String.format(
                "insert into booking select %s, c.customerid, %s,%s,'%s',%s,%s from customer c where c.fname = '%s' and c.lname = '%s' limit 1",
                esql.getNextId(esql, "bID", "booking"), hotelID, roomNo, date, numberOfPeople,
                String.format("%.2f", price), fname, lname);
 
        try {
            esql.executeUpdate(sql);
            System.out.println("The booking was succesfully created!");
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("The booking couldn't be created.");
        }
 
    }// end bookRoom
 
    public static void assignHouseCleaningToRoom(DBProject esql) {
        // Given Staff SSN, HotelID, roomNo Assign the staff to the room
        try {
 
            System.out.print("\tEnter staff id: ");
            String staffID = in.readLine();
 
            System.out.print("\tEnter hotelID: ");
            String hotelid = in.readLine();
 
            System.out.print("\tEnter room number: ");
            String roomNo = in.readLine();
 
            String sql = String.format("INSERT INTO Assigned VALUES ('%s','%s','%s','%s')",
                    esql.getNextId(esql, "asgID", "Assigned"), staffID, hotelid, roomNo);
 
            esql.executeUpdate(sql);
 
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end assignHouseCleaningToRoom
 
    public static void repairRequest(DBProject esql) {
        // Given a hotelID, Staff SSN, roomNo, repairID , date create a repair request
        // in the DB
        int ssn, repairID;
        String date, description;
 
        System.out.print("Enter SSN: ");
        ssn = readInt();
 
        System.out.print("Enter repair ID: ");
        repairID = readInt();
 
        System.out.print("Enter date: ");
        date = readText();
 
        System.out.print("Enter description: ");
        description = readText();
 
        String sql = String.format("INSERT INTO request VALUES(%s,%s,%s,'%s','%s')",
                esql.getNextId(esql, "reqID", "request"), ssn, repairID, date, description);
 
        try {
            esql.executeUpdate(sql);
            System.out.println("The request was succesfully created!");
        } catch (Exception e) {
            System.out.println("The request couldn't be created.");
        }
 
    }// end repairRequest
 
    public static void numberOfAvailableRooms(DBProject esql) {
        // Given a hotelID, get the count of rooms available
        try {
            System.out.print("\tEnter Hotel ID: ");
            String hotelid = in.readLine();
 
            String query = String.format("SELECT COUNT(*) FROM Room R "
                    + "WHERE R.hotelid = '%s' and R.roomNo not in (SELECT roomNo FROM Booking B WHERE B.hotelid = '%s');",
                    hotelid, hotelid);
 
            esql.executeQuery(query);
 
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end numberOfAvailableRooms
 
    public static void numberOfBookedRooms(DBProject esql) {
        // Given a hotelID, get the count of rooms booked
        int hotelID;
        System.out.print("Enter hotel ID: ");
        hotelID = readInt();
 
        String sql = String.format("Select count(*) as TotalBookings from booking where hotelid=%s", hotelID);
 
        try {
            esql.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("There was an error.");
        }
    }// end numberOfBookedRooms
 
    public static void listHotelRoomBookingsForAWeek(DBProject esql) {
        // Given a hotelID, date - list all the rooms available for a week(including the
        // input date)
        try {
 
            System.out.print("\tEnter hotelID: ");
            String hotelid = in.readLine();
 
            System.out.print("\tEnter date(mm/dd/yyyy): ");
            String date = in.readLine();
 
            Date date1 = new SimpleDateFormat("MM/dd/yyyy").parse(date);
            Calendar c = Calendar.getInstance();
            c.setTime(date1);
            c.add(Calendar.DATE, 7);
            DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
            String weeklater = df.format(c.getTime());
 
            String sql = String.format(
                    "SELECT * from Booking WHERE hotelid = '%s' and bookingdate BETWEEN '%s' AND '%s'", hotelid, date,
                    weeklater);
            esql.executeQuery(sql);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end listHotelRoomBookingsForAWeek
 
    public static void topKHighestRoomPriceForADateRange(DBProject esql) {
        // List Top K Rooms with the highest price for a given date range
        int k;
        String from, to;
 
        System.out.print("Enter k: ");
        k = readInt();
 
        System.out.print("Enter starting date: ");
        from = readText();
 
        System.out.print("Enter ending date: ");
        to = readText();
 
        String sql = String.format("select hotelID, roomNo, price, bookingDate from booking where bookingDate between"
                + " '%s' and '%s' order by price DESC limit %s", from, to, k);
 
        try {
            esql.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("There was an error.");
        }
 
    }// end topKHighestRoomPriceForADateRange
 
    public static void topKHighestPriceBookingsForACustomer(DBProject esql) {
        // Given a customer Name, List Top K highest booking price for a customer
        try {
 
            System.out.print("\tEnter customer's first name: ");
            String fName = in.readLine();
            System.out.print("\tEnter customer's last name: ");
            String lName = in.readLine();
            System.out.print("\tEnter k: ");
            String k = in.readLine();
 
            String sql = String.format("SELECT B.price FROM Booking B, Customer C "
                    + "WHERE C.customerid = B.customer AND C.fName = '%s' AND C.lName = '%s' "
                    + "ORDER BY B.price DESC LIMIT %s; ", fName, lName, k);
 
            esql.executeQuery(sql);
 
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end topKHighestPriceBookingsForACustomer
 
    public static void totalCostForCustomer(DBProject esql) {
        // Given a hotelID, customer Name and date range get the total cost incurred by
        // the customer
        int hotelID;
        String fname, lname, start, end;
 
        System.out.print("Enter hotel ID: ");
        hotelID = readInt();
 
        System.out.println("Enter customer's first name: ");
        fname = readText();
 
        System.out.println("Enter customer's last name: ");
        lname = readText();
 
        System.out.println("Enter start date: ");
        start = readText();
 
        System.out.println("Enter end date: ");
        end = readText();
 
        try {
            String sql = String.format("select customerid from customer where fname = '%s' and lname='%s' limit 1",
                    fname, lname);
            int customerid = 0;
 
            Statement stmt = esql._connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next())
                customerid = rs.getInt("customerid");
 
            sql = String.format(
                    "select COALESCE(sum(price), 0) as TotalIncurred from booking  where hotelID = %s and customer = %s and bookingdate between '%s' and '%s' ",
                    hotelID, customerid, start, end);
            esql.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("There was an error.");
            System.out.println(e.getMessage());
        }
 
    }// end totalCostForCustomer
 
    public static void listRepairsMade(DBProject esql) {
        // Given a Maintenance company name list all the repairs along with repairType,
        // hotelID and roomNo
        try {
            String mcomp = "";
 
            System.out.print("\tEnter Maintenance company name: ");
            mcomp = in.readLine();
 
            String sql = String
                    .format("SELECT r.rid, r.hotelid, r.roomNo, r.repairType FROM maintenancecompany mc, repair r "
                            + "WHERE mc.name = '%s' AND mc.cmpid = r.mcompany;", mcomp);
 
            esql.executeQuery(sql);
 
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end listRepairsMade
 
    public static void topKMaintenanceCompany(DBProject esql) {
        // List Top K Maintenance Company Names based on total repair count (descending
        // order)
        int k;
 
        System.out.print("Enter k: ");
        k = readInt();
 
        String sql = String.format(
                "select m.name, count(m.cmpID) as RepairCount from MaintenanceCompany m, repair r where m.cmpID = r.mCompany \r\n"
                        + "group by m.cmpID\r\n" + "order by RepairCount DESC\r\n" + "limit 5",
                k);
 
        try {
            esql.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("There was an error.");
        }
 
    }// end topKMaintenanceCompany
 
    public static void numberOfRepairsForEachRoomPerYear(DBProject esql) {
        // Given a hotelID, roomNo, get the count of repairs per year
        try {
            System.out.print("\tEnter Hotel ID: ");
            String hotelid = in.readLine();
 
            System.out.print("\tEnter room number: ");
            String roomno = in.readLine();
 
            String sql = String.format(
                    "SELECT COUNT(DATE_PART('year', repairdate)) as number_of_repairs, DATE_PART('year', repairdate) as year "
                            + "FROM repair r WHERE r.hotelid = %s and r.roomno = %s GROUP BY DATE_PART('year', repairdate);",
                    hotelid, roomno);
 
            esql.executeQuery(sql);
 
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
 
    }// end listRepairsMade
 
}// end DBProject
