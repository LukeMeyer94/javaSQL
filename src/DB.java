import com.mysql.jdbc.MysqlErrorNumbers;

import java.sql.*;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by lukemeyer on 4/28/17.
 */
public class DB {

    private Connection cnx;
    private String role;
    private Scanner s;
    private static final int RETRIES = 5;


    /**
     * Constructor calls methods to connect to, create tables, and insert data into db
     * Following success of these, the constructor calls user login method
     * @throws InterruptedException
     */
    public DB () throws InterruptedException {
        s = new Scanner(System.in);
        connectToDB();
        dbInterface();
        System.out.println("Welcome to the shipping database program");
        loginUser();
    }

    private void dbInterface() throws InterruptedException {
        System.out.println("Enter \"Build\" to create tables, or press enter to continue");
        String input = s.nextLine();
        if (input.toLowerCase().equals("build")){
            createTables();
            insertSampleData();
        }
    }

    /**
     * Method connectToDB establishes a connection with the remote MySQL DB
     */
    private void connectToDB() {
        Map<String, String> env = System.getenv();
        String passwd = env.get("DBPASSWORD");
        try {
            System.out.println("Connecting...");
            // Establish a connection to your database
            cnx = DriverManager.getConnection("jdbc:mysql://dbdev.divms.uiowa.edu:3306/db_lrmeyr",
                    "lrmeyr",
                    passwd);
            System.out.println("Connected to db");

            // For this connection, set isolation level to SERIALIZABLE
            cnx.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // Turn off autocommit (i.e., commit after every executed statement)
            // When autocommit is off, you must call commit to execute your statement(s).
            // In the java.sql (JDBC) API, you do not need to begin a transaction. A new
            // transaction begins when the connection is established and after each commit().
            cnx.setAutoCommit(false);
        } catch (SQLException e) {
            System.out.println("failed to connect to database "+e);
            System.exit(1);
            try {
                cnx.close();
            } catch (Exception e2) {}
        }
    }

    /**
     * Method createTables creates tables in the DB for ZIP, Customer, Truck, and Package
     * @throws InterruptedException
     */
    private void createTables () throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do{
            try {

                Statement dbStatement = cnx.createStatement();
                dbStatement.execute("drop table if exists Package");
                dbStatement.execute("drop table if exists Truck");
                dbStatement.execute("drop table if exists Customer");
                dbStatement.execute("drop table if exists Zip");
                dbStatement.close();
                Statement zipDBStatement= cnx.createStatement();
                Statement customerDBStatement = cnx.createStatement();
                Statement truckDBStatement = cnx.createStatement();
                Statement packageDBStatement = cnx.createStatement();
                zipDBStatement.execute("create table if not exists Zip (\n" +
                        "  code int NOT NULL,\n" +
                        "  city varchar(20) NOT NULL,\n" +
                        "  PRIMARY KEY (code)\n" +
                        ")");
                customerDBStatement.execute("create table if not exists Customer (\n" +
                        "  id varchar(20) NOT NULL,\n" +
                        "  name varchar(20) NOT NULL,\n" +
                        "  address varchar(50) NOT NULL,\n" +
                        "  zipcode int NOT NULL,\n" +
                        "  PRIMARY KEY (id),\n" +
                        "  FOREIGN KEY (zipcode) references Zip(code)\n" +
                        ")");
                truckDBStatement.execute("create table if not exists Truck (\n" +
                        "  tid varchar(20) NOT NULL,\n" +
                        "  home_city_zipcode int,\n" +
                        "  weight_capacity int,\n" +
                        "  current_weight int,\n" +
                        "  PRIMARY KEY (tid),\n" +
                        "  FOREIGN KEY (home_city_zipcode) references Zip(code)\n" +
                        ")");
                packageDBStatement.execute("create table if not exists Package (\n" +
                        "  pid varchar(20) NOT NULL,\n" +
                        "  weight int NOT NULL,\n" +
                        "  volume int NOT NULL,\n" +
                        "  cost int,\n" +
                        "  origin_city_zip int NOT NULL,\n" +
                        "  sender varchar(20),\n" +
                        "  shipping_date DATE NOT NULL," +
                        "  receiving_date DATE," +
                        "  status varchar(50),\n" +
                        "  delivery_truck varchar(20),\n" +
                        "  receiver_id varchar(20),\n" +
                        "  PRIMARY KEY (pid),\n" +
                        "  FOREIGN KEY (origin_city_zip) references Zip(code),\n" +
                        "  FOREIGN KEY (receiver_id) references Customer (id),\n" +
                        "  FOREIGN KEY (delivery_truck) references Truck (tid)\n" +
                        ")");
                zipDBStatement.close();
                customerDBStatement.close();
                truckDBStatement.close();
                packageDBStatement.close();
                cnx.commit();
                System.out.println("Tables created");
            } catch ( SQLException e ){
                if(e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_DEADLOCK){
                    error = true;
                    System.out.println("Deadlock occurred, " + R + " more attempts remaining, trying again...");
                    Thread.sleep(1000);
                }
                else {
                    e.printStackTrace();
                }
            }

        }
        while (error == true && R-- > 0);

    }

    /**
     * Method insertSampleData inserts fake data into the DB for functionality testing
     * @throws InterruptedException
     */
    private void insertSampleData () throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                insertZipData(52245,"Iowa City");
                insertZipData(60137,"Glen Ellyn");
                insertCustomerData("123","Luke Meyer","314 N Clinton St",52245);
                insertTruckData("1",52245,100,95);
                insertTruckData("2",52245,100,0);
                insertPackageData("1",6,5,20,60137,"Amazon","2016-01-01","2016-01-11","Awaiting Transit","2","123");
                insertPackageData("2",5,6,20,52245,"Amazon","2016-01-15","2016-01-21","In Transit","1","123");
                insertPackageData("3",7,9,20,52245,"Amazon","2016-02-15","2016-02-21","Awaiting Transit","1","123");

                System.out.println("Sample Data inserted");

                cnx.commit();

            } catch ( SQLException e ){
                if(e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_DEADLOCK){
                    error = true;
                    System.out.println("Deadlock occurred, " + R + " more attempts remaining, trying again...");
                    Thread.sleep(1000);
                }
                else {
                    e.printStackTrace();
                }
            }

        }
        while (error == true && R-- > 0);

    }

    private void insertPackageData( String id, int weight, int vol, int cost, int oZip, String sender, String sDate, String rDate, String status, String tid, String cid) throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                PreparedStatement preparedInsertPackage = cnx.prepareStatement("insert into Package (pid, weight, volume, cost, origin_city_zip, sender, shipping_date, receiving_date, status, delivery_truck, receiver_id) select * from ( select ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) as tmp where not exists (select pid from Package where pid=?) limit 1");
                preparedInsertPackage.setString(1,id);
                preparedInsertPackage.setInt(2,weight);
                preparedInsertPackage.setInt(3,vol);
                preparedInsertPackage.setInt(4,cost);
                preparedInsertPackage.setInt(5,oZip);
                preparedInsertPackage.setString(6,sender);
                preparedInsertPackage.setString(7,sDate);
                preparedInsertPackage.setString(8,rDate);
                preparedInsertPackage.setString(9,status);
                preparedInsertPackage.setString(10,tid);
                preparedInsertPackage.setString(11,cid);
                preparedInsertPackage.setString(12,id);
                preparedInsertPackage.execute();
                preparedInsertPackage.close();

            } catch ( SQLException e ){
                if(e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_DEADLOCK){
                    error = true;
                    System.out.println("Deadlock occurred, " + R + " more attempts remaining, trying again...");
                    Thread.sleep(1000);
                }
                else {
                    e.printStackTrace();
                }
            }

        }
        while (error == true && R-- > 0);
    }

    private void insertTruckData( String id, int zip, int cap, int w) throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                PreparedStatement preparedInsertTruck = cnx.prepareStatement("insert into Truck (tid, home_city_zipcode, weight_capacity, current_weight) select * from (select ?,?,?,?) as tmp where not exists (select tid from Truck where tid=?) limit 1");
                preparedInsertTruck.setString(1,id);
                preparedInsertTruck.setInt(2,zip);
                preparedInsertTruck.setInt(3,cap);
                preparedInsertTruck.setInt(4,w);
                preparedInsertTruck.setString(5,id);
                preparedInsertTruck.execute();
                preparedInsertTruck.close();

            } catch ( SQLException e ){
                if(e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_DEADLOCK){
                    error = true;
                    System.out.println("Deadlock occurred, " + R + " more attempts remaining, trying again...");
                    Thread.sleep(1000);
                }
                else {
                    e.printStackTrace();
                }
            }

        }
        while (error == true && R-- > 0);
    }

    private void insertCustomerData( String id, String name, String address, int zip) throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                PreparedStatement preparedInsertCust = cnx.prepareStatement("insert into Customer (id, name, address, zipcode) select * from (select ?,?,?,?) as tmp where not exists (select id from Customer where id=?) limit 1");
                preparedInsertCust.setString(1,id);
                preparedInsertCust.setString(2,name);
                preparedInsertCust.setString(3,address);
                preparedInsertCust.setInt(4,zip);
                preparedInsertCust.setString(5,id);
                preparedInsertCust.execute();

            } catch ( SQLException e ){
                if(e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_DEADLOCK){
                    error = true;
                    System.out.println("Deadlock occurred, " + R + " more attempts remaining, trying again...");
                    Thread.sleep(1000);
                }
                else {
                    e.printStackTrace();
                }
            }

        }
        while (error == true && R-- > 0);
    }

    private void insertZipData(int code, String city) throws InterruptedException{
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                PreparedStatement preparedInsertZIP =cnx.prepareStatement("insert into Zip (code, city) select * from (select ?, ?) as tmp where not exists (select code from Zip where code=?) limit 1");
                preparedInsertZIP.setInt(1,code);
                preparedInsertZIP.setString(2,city);
                preparedInsertZIP.setInt(3,code);
                preparedInsertZIP.execute();
                preparedInsertZIP.close();

            } catch ( SQLException e ){
                if(e.getErrorCode() == MysqlErrorNumbers.ER_LOCK_DEADLOCK){
                    error = true;
                    System.out.println("Deadlock occurred, " + R + " more attempts remaining, trying again...");
                    Thread.sleep(1000);
                }
                else {
                    e.printStackTrace();
                }
            }

        }
        while (error == true && R-- > 0);
    }

    /**
     * Recursive Method loginUser allows a user to login as a manager or customer
     * @throws InterruptedException
     */
    private void loginUser() throws InterruptedException {
        System.out.println("Please log in (type manager or customer)");
        String roleInput = s.nextLine();
        if ( roleInput.toLowerCase().equals("manager")){
            role = "manager";
            System.out.println("Welcome Manager");
            Manager m = new Manager(cnx,RETRIES);
            m.close();
            return;
        } else if ( roleInput.toLowerCase().equals("customer") ) {
            role = "customer";
            System.out.println("Welcome Customer");
            Customer c = new Customer(cnx,RETRIES);
            c.close();
            return;
        } else {
            System.out.println("Try again");
            loginUser();
        }
    }


    public void close() {
        try {
            cnx.close();
        } catch (SQLException e) {}
    }
}
