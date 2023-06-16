package src;

import src.exceptions.DBAppException;
import java.util.*;
import src.classes.*;


public class Tests {
    DBApp dbApp = new DBApp();

    public Tests() {

    }

    public void createTable() throws DBAppException {
        // Create table
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
        
        dbApp.createTable("Product", "ProductID", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
    }
    
    public void testTable() throws DBAppException {
        // Test Table class
        Table tbl = new Table("Product");
        ArrayList<String> colNames = tbl.getColNames();

        for (String colName : colNames) {
            System.out.println(colName);
        }
    }

    public void insertTable() throws DBAppException {
        // Test insert into table
        Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();
        Hashtable<String,Object> htblColNameValue2 = new Hashtable<String, Object>();

        htblColNameValue.put("ProductID", 10);
        htblColNameValue.put("ProductName", "Test Product");
        htblColNameValue.put("ProductPrice", 1000.00);
        
        htblColNameValue2.put("ProductID", 100);
        htblColNameValue2.put("ProductPrice", 80000.20);
        htblColNameValue2.put("ProductName", "Macbook");


        // dbApp.insertIntoTable("Product", htblColNameValue);
        dbApp.insertIntoTable("Product", htblColNameValue2);
    }

    public void updateTable() throws DBAppException {
        Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();

        // htblColNameValue.put("ProductID", 200);
        htblColNameValue.put("ProductPrice", 12.00);
        //  htblColNameValue.put("ProductPrice", 1000.00);

        dbApp.updateTable("Product", "200", htblColNameValue);
    }

    public void deleteFromTable() throws DBAppException {
        Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();
        htblColNameValue.put("ProductID", 10);
        htblColNameValue.put("ProductName", "Test Product");

        dbApp.deleteFromTable("Product", htblColNameValue);
    }

    public void createIndex() throws DBAppException {
        dbApp.createIndex("Product", new String[] {"ProductPrice", "ProductID"});
    }
}
