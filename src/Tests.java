package src;

import src.exceptions.DBAppException;
import java.util.*;
import src.classes.*;


public class Tests {
    DBApp dbApp = new DBApp();

    public Tests() {

    }

    public void createTable() {
        // Create table
        try {
            Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
            htblColNameType.put("ProductID", "java.lang.Integer");
            htblColNameType.put("ProductName", "java.lang.String");
            htblColNameType.put("ProductPrice", "java.lang.Double");
            
            Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
            htblColNameMin.put("ProductID", "0");
            htblColNameMin.put("ProductName", "A");
            htblColNameMin.put("ProductPrice", "0");
            
            Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
            htblColNameMax.put("ProductID", "1000");
            htblColNameMax.put("ProductName", "ZZZZZZZZZZZ");
            htblColNameMax.put("ProductPrice", "100000");
            
            Hashtable<String, String> htblForeignKeys = new Hashtable<String, String>();
            
            String[] computedCols = {};
            
            dbApp.createTable("Product", " ProductID ", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
        } catch (DBAppException e) {
            System.err.println("Can't Create Table.");
            e.printStackTrace();
        }
    }
    
    public void testTable() {
        // Test Table class
        try {
            Table tbl = new Table("Product");
            ArrayList<String> colNames = tbl.getColNames();

            for (String colName : colNames) {
                System.out.println(colName);
            }
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }

    public void insertTable() {
        // Test insert into table
        try {
            Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();

            htblColNameValue.put("ProductID", 200);
            htblColNameValue.put("ProductName", "Test Product");
            htblColNameValue.put("ProductPrice", 1000.00);

            dbApp.insertIntoTable("Product", htblColNameValue);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
