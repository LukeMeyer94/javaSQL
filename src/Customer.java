import com.mysql.jdbc.MysqlErrorNumbers;

import java.sql.*;
import java.util.Scanner;

/**
 * Created by lukemeyer on 5/4/17.
 */
public class Customer {

    private Connection cnx;
    private String customerID;
    private static int RETRIES;
    private Scanner s;

    /**
     * Constuctor assigns class fields and launches customer ID login
     * @param con
     * @param r
     * @throws InterruptedException
     */
    public Customer (Connection con, int r) throws InterruptedException {
        RETRIES = r;
        cnx = con;
        s = new Scanner(System.in);
        loginCustomer();
    }

    /**
     * Method loginCustomer allows a customer to log in with their customer ID
     * @throws InterruptedException
     */
    private void loginCustomer() throws InterruptedException {
        System.out.println("Please enter your customer ID");
        String idInput = s.nextLine();
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findUserInDB = cnx.createStatement();
                ResultSet findUserResultSet = findUserInDB.executeQuery("select * from Customer where id=\"" + idInput + "\"");
                if(findUserResultSet.next()){
                    System.out.println("Welcome " + findUserResultSet.getString("name"));
                    customerID = findUserResultSet.getString("id");
                    customerInterface();
                    return;
                } else {
                    System.out.println("User not found. Please try again");
                    loginCustomer();
                }
            }  catch ( SQLException e ){
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
     * Recursive method customerInterface allows the customer to select between several options
     * @throws InterruptedException
     */
    private void customerInterface () throws InterruptedException {
        System.out.println("\n\nCustomer Interface, please enter one of the following or type \"quit\" to terminate program:\n" +
                "Look up status of all your packages: PL\n" +
                "Schedule a pickup: SP");
        String command = s.nextLine();
        System.out.println();
        switch (command.toLowerCase()) {
            case "quit":
                return;
            case "pl":
                packageStatusLookup();
                customerInterface();
                break;
            case "sp":
                packageStatusLookup();
                schedulePickup();
                customerInterface();
                break;
            default:
                System.out.println("Command not recognized, try again");
                customerInterface();
                return;
        }
    }

    /**
     * Method packageStatusLookup displays all of a customer's packages and their statuses
     * @throws InterruptedException
     */
    private void packageStatusLookup() throws InterruptedException {
        System.out.println("Here are your packages...");
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findUserPackages = cnx.createStatement();
                ResultSet findUserPackagesResultSet = findUserPackages.executeQuery("Select * from Package where receiver_id=\"" + customerID + "\"");
                while(findUserPackagesResultSet.next()){
                    System.out.println("Package ID: " + findUserPackagesResultSet.getString("pid") + ", Status: " + findUserPackagesResultSet.getString("status"));
                }
                cnx.commit();
            }  catch ( SQLException e ){
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
     * Method schedulePickup allows a customer to update a package status to awaiting pickup
     * @throws InterruptedException
     */
    private void schedulePickup() throws InterruptedException {
        System.out.println("Please enter the ID of that package you would like to schedule for pickup");
        String packageID = s.nextLine();
        updateStatusForPickup(packageID);
        calculatePackageCost(packageID);
        packageStatusLookup();
    }

    private void updatePackageCost(String pid, int cost) throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement updatePackage = cnx.createStatement();
                updatePackage.execute("update Package set cost=\"" + cost + "\" where pid=\"" + pid + "\"");
                cnx.commit();
            }  catch ( SQLException e ){
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
     * Method calculatePackageCost takes in a package ID number and calculates the cost based on weight, volume, and the difference between the origin and destination ZIP
     * @param pid
     * @throws InterruptedException
     */
    private void calculatePackageCost(String pid) throws InterruptedException {
        int destZip = getCustomerZip();
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement getPackage = cnx.createStatement();
                ResultSet getPackageResults = getPackage.executeQuery("select * from Package where pid=\"" + pid + "\"");
                getPackageResults.next();
                int originZip = Integer.valueOf(getPackageResults.getString("origin_city_zip"));
                int weight = getPackageResults.getInt("weight");
                int volume = getPackageResults.getInt("volume");
                // Math to calculate cost?
                int cost = Math.abs(originZip-destZip) + (weight + volume);
                System.out.println("Package cost due on pickup: $" + cost);
                updatePackageCost(pid,cost);
            }  catch ( SQLException e ){
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
     * Method updateStatusForPickup takes a package ID and status and updates the package's status accordingly
     * @param pid
     * @throws InterruptedException
     */
    private void updateStatusForPickup(String pid) throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement updatePackage = cnx.createStatement();
                updatePackage.execute("update Package set status=\"Awaiting Pickup\" where pid=\"" + pid + "\"");
                cnx.commit();
            }  catch ( SQLException e ){
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
     * Method getCustomerZip returns the zipcode of the current customer
     * @return
     * @throws InterruptedException
     */
    private int getCustomerZip() throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                ResultSet r = cnx.createStatement().executeQuery("Select * from Customer where id=" + customerID);
                r.next();
                return r.getInt("zipcode");
            }  catch ( SQLException e ){
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
        return 0;
    }

    public void close() {
        s.close();
        try {
            cnx.close();
        } catch (SQLException e) {}
    }
}
