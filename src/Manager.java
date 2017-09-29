import com.mysql.jdbc.MysqlErrorNumbers;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
/**
 * Created by lukemeyer on 5/4/17.
 */
public class Manager {

    private Connection cnx;
    private Scanner s;
    private static int RETRIES;

    /**
     * Constructor initializes class fields and starts manager interface
     * @param con
     * @param r
     * @throws InterruptedException
     */
    public Manager (Connection con, int r) throws InterruptedException{
        RETRIES = r;
        cnx = con;
        s = new Scanner(System.in);
        managerInterface();
    }

    /**
     * Recursive method managerInterface allows the manager to select between several options
     * @throws InterruptedException
     */
    private void managerInterface() throws InterruptedException {
        System.out.println("\n\nManager Interface\nPlease enter one of the following or type \"quit\" to terminate program:\n" +
                "Update the status on a package: UP\n" +
                "List scheduled pickups: LSP\n" +
                "Calculate number of packages for a month: CNPM\n" +
                "Calculate average time to move packages between 2 cities: CATMP\n" +
                "Add package to a truck: APTT");
        String command = s.nextLine();
        System.out.println();
        switch (command.toLowerCase()) {
            case "quit":
                return;
            case "up":
                updatePackageStatus();
                managerInterface();
                break;
            case "lsp":
                listScheduledPickups();
                managerInterface();
                break;
            case "cnpm":
                calculateNumPackagesMonth();
                managerInterface();
                break;
            case "catmp":
                calculateAverageTimeBetweenCities();
                managerInterface();
                break;
            case "aptt":
                addPackageToTruck();
                managerInterface();
                break;
            default:
                System.out.println("Command not recognized, try again");
                managerInterface();
                return;
        }
    }

    /**
     * Method updatePackageStatus allows a manager to update the status of any package
     * @throws InterruptedException
     */
    private void updatePackageStatus() throws InterruptedException {
        System.out.println("Here's a list of packages available for a status update");

        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findAllPackages = cnx.createStatement();
                ResultSet findAllPackagesResultSet = findAllPackages.executeQuery("Select * from Package");
                while(findAllPackagesResultSet.next()){
                    String customerName = getCustomerNameFromID(findAllPackagesResultSet.getString("receiver_id"));
                    System.out.println("Package ID: " + findAllPackagesResultSet.getString("pid") + ", Customer: " + customerName + ", Status: " + findAllPackagesResultSet.getString("status"));
                }
                cnx.commit();
                System.out.println("Please enter the ID of the package you want to update");
                String pid = s.nextLine();
                System.out.println("Please enter the packages new status");
                String status = s.nextLine();
                updateStatus(pid,status);
                System.out.println("Package status updated");
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


    /**
     * Method getCustomerNameFromID takes in a customer ID and returns the customer's name
     * @param id
     * @return
     * @throws InterruptedException
     */
    private String getCustomerNameFromID(String id) throws InterruptedException{
        boolean error = false;
        int R = RETRIES;
        do {
            try{
                ResultSet r = cnx.createStatement().executeQuery("Select * from Customer where id=" + id);
                r.next();
                return r.getString("name");
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
        return "";
    }

    /**
     * Method updateStatus takes a package ID and status and updates the package's status accordingly
     * @param pid
     * @param status
     * @throws InterruptedException
     */
    private void updateStatus(String pid, String status) throws InterruptedException {
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement updatePackage = cnx.createStatement();
                updatePackage.execute("update Package set status=\"" + status + "\" where pid=\"" + pid + "\"");
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

    /**
     * Method listScheduledPickups displays all packages awaiting pickup
     * @throws InterruptedException
     */
    private void listScheduledPickups() throws InterruptedException {
        System.out.println("Here's a list of packages available for pickup");
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findAllPackages = cnx.createStatement();
                ResultSet findAllPackagesResultSet = findAllPackages.executeQuery("Select * from Package where status=\"Awaiting Pickup\"");
                if(findAllPackagesResultSet.next()){
                    System.out.println("Package ID: " + findAllPackagesResultSet.getString("pid") + ", Customer: " + findAllPackagesResultSet.getString("receiver_id") + ", Status: " + findAllPackagesResultSet.getString("status"));
                    while(findAllPackagesResultSet.next()){
                        System.out.println("Package ID: " + findAllPackagesResultSet.getString("pid") + ", Customer: " + findAllPackagesResultSet.getString("receiver_id") + ", Status: " + findAllPackagesResultSet.getString("status"));
                    }
                }
                else {
                    System.out.println("No packages awaiting pickup");
                    cnx.rollback();
                }
                return;

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
     * Method calculateNumPackagesMonth allows for input of a year and a month and calls calculateNumPackages to show user the result
     * @throws InterruptedException
     */
    private void calculateNumPackagesMonth() throws InterruptedException {
        System.out.println("Please enter the year: ");
        String input = s.nextLine();
        int year = 0;
        int month = 0;
        try{
            year = Integer.parseInt(input);

        } catch(Exception e){
            System.out.println("Invalid input, try again");
            calculateNumPackagesMonth();
            return;
        }

        System.out.println("Please enter the month");
        input = s.nextLine();
        try{
            month = Integer.parseInt(input);
        } catch (Exception e){
            System.out.println("Invalid input, try again");
            calculateNumPackagesMonth();
            return;
        }
        System.out.println("Year: " + year + ", month: " + month);
        calculateNumPackages(year,month);
    }

    /**
     * Method calculateNumPackages takes in a year and a month and displays to the user how many packages were sent that month
     */
    private void calculateNumPackages(int year, int month) throws InterruptedException {
        int count = 0;
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findPackages = cnx.createStatement();
                ResultSet findPackagesResultSet = findPackages.executeQuery("Select * from Package where YEAR(shipping_date)=" + year + " and MONTH(shipping_date)=" + month);
                while(findPackagesResultSet.next()){
                    count++;
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
        System.out.println(count + " packages total for " + month + "-" + year);
    }

    /**
     * Method addPackageToTruck shows users all packages awaiting transit, and allows them to add it to the first truck with available space
     * @throws InterruptedException
     */
    private void addPackageToTruck() throws InterruptedException {
        System.out.println("Here's a list of packages available for shipping");
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findAllPackages = cnx.createStatement();
                ResultSet findAllPackagesResultSet = findAllPackages.executeQuery("Select * from Package where status=\"Awaiting Transit\"");
                while(findAllPackagesResultSet.next()){
                    String customerName = getCustomerNameFromID(findAllPackagesResultSet.getString("receiver_id"));
                    System.out.println("Package ID: " + findAllPackagesResultSet.getString("pid") + ", Customer: " + customerName + ", Status: " + findAllPackagesResultSet.getString("status"));
                }
                System.out.println("Please enter the ID of the package you want to add to a truck");
                String pid = s.nextLine();
                Statement findPackage = cnx.createStatement();
                ResultSet getPackage = findPackage.executeQuery("Select * from Package where pid=\"" + pid + "\"");
                getPackage.next();
                System.out.println(getPackage.getInt("weight"));
                Statement findTruck = cnx.createStatement();
                ResultSet TruckResults = findTruck.executeQuery("Select * from Truck where weight_capacity > (current_weight+" + getPackage.getInt("weight") + ")");
                if(TruckResults.next()){
                    System.out.println(TruckResults.getInt("weight_capacity"));
                    Statement updateTruck = cnx.createStatement();
                    updateTruck.execute("update Truck set current_weight=(current_weight+" + getPackage.getInt("weight") + ") where tid=\"" + TruckResults.getString("tid") + "\"");
                    updateStatus(pid,"In Transit");
                    cnx.commit();
                    System.out.println("Package status updated to \"In Transit\"");
                } else {
                    System.out.println("Sorry, no trucks available");
                }

                Statement showTrucks = cnx.createStatement();
                ResultSet showTrucksResults = showTrucks.executeQuery("select * from Truck");
                System.out.println("Here are all of your trucks");
                while(showTrucksResults.next()){
                    System.out.println("Truck ID:" + showTrucksResults.getString("tid") + ", Weight capacity = " + showTrucksResults.getInt("weight_capacity") + ", current weight:" + showTrucksResults.getInt("current_weight"));
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


    private void calculateAverageTimeBetweenCities() throws InterruptedException {
        System.out.println("Please enter the first city");
        String origin = s.nextLine();
        System.out.println("Please enter the second city");
        String dest = s.nextLine();
        int [] originZips = getZipsFromCity(origin);
        int [] destZips = getZipsFromCity(dest);
        int sum = 0;
        int count = 0;
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                for (int i = 0; i < originZips.length; i++) {
                    Statement getCustomersInOriginZip = cnx.createStatement();
                    ResultSet getCustomersInOriginZIPResults = getCustomersInOriginZip.executeQuery("Select id from Customer where zipcode=" + originZips[i]);
                    for (int j = 0; j < destZips.length; j++) {
                        Statement getCustomersInDestZIP = cnx.createStatement();
                        ResultSet getCustomersInDestZIPResults = getCustomersInDestZIP.executeQuery("Select id from Customer where zipcode=" + destZips[j]);
                        while( getCustomersInDestZIPResults.next() ){

                            System.out.println(getCustomersInDestZIPResults.getString("id") + " " + originZips[i]);
                            Statement getTimeDiff = cnx.createStatement();
                            ResultSet getTimeDiffResultSet = getTimeDiff.executeQuery("Select * from Package " +
                                    "where origin_city_zip=" + originZips[i] + " AND receiver_id=\"" + getCustomersInDestZIPResults.getString("id") + "\"");
                            while(getTimeDiffResultSet.next()){
                                System.out.println("found package");
                                System.out.println(getTimeDiffResultSet.getString("pid"));
//                            sum += Math.abs(getTimeDiffResultSet.getInt("diff"));
                                count++;
                            }
                        }
                        while(getCustomersInOriginZIPResults.next()){

                            System.out.println(getCustomersInOriginZIPResults.getString("id") + " - " + destZips[j]);
                            Statement getTimeDiff2 = cnx.createStatement();
                            ResultSet getTimeDiffResultSet2 = getTimeDiff2.executeQuery("Select * from Package" +
                                    "where origin_city_zip=" + destZips[j] + " AND receiver_id=\"" + getCustomersInOriginZIPResults.getString("id") + "\"");
                            while(getTimeDiffResultSet2.next()){
                                System.out.println("found package");
                                sum += Math.abs(getTimeDiffResultSet2.getInt("diff"));
                                count++;
                            }
                        }
                    }
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
        System.out.println("sum: " + sum + ", count: " + count);

    }

    private int [] getZipsFromCity( String city ) throws InterruptedException {
        List<Integer> result = new ArrayList<>();
        boolean error = false;
        int R = RETRIES;
        do {
            try {
                Statement findZips = cnx.createStatement();
                ResultSet findZipsResultSet = findZips.executeQuery("Select * from Zip where city=\"" + city + "\"");
                while(findZipsResultSet.next()){
                    result.add(findZipsResultSet.getInt("code"));
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
        int [] returning = new int[result.size()];
        for ( int i = 0; i < result.size(); i ++){
            returning[i] = result.get(i);
        }
        return returning;
    }

    public void close() {
        s.close();
        try {
            cnx.close();
        } catch (SQLException e) {}
    }


}
