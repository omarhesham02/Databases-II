package src;

import src.exceptions.DBAppException;

import java.sql.Date;
import java.util.*;
import src.classes.*;


public class Tests {
    DBApp dbApp = new DBApp();

    public Tests() {

    }

    public void createTable() throws DBAppException {
        // Create table Product
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
        
        // dbApp.createTable("Product", "ProductID", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
        
        // Create table Sale
        Hashtable<String, String> htblColNameType2 = new Hashtable<String, String>();
        htblColNameType2.put("SaleID", "java.lang.Integer");
        htblColNameType2.put("SaleDate", "java.util.Date");
        htblColNameType2.put("ProductID", "java.lang.Integer");
        htblColNameType2.put("Quantity", "java.lang.Integer");
        htblColNameType2.put("TotalAmount", "java.lang.Double");
        
        Hashtable<String, String> htblColNameMin2 = new Hashtable<String, String>();
        htblColNameMin2.put("SaleID", "0");
        htblColNameMin2.put("SaleDate", "01.01.2000");
        htblColNameMin2.put("ProductID", "0");
        htblColNameMin2.put("Quantity", "1");
        htblColNameMin2.put("TotalAmount", "0");
        
        Hashtable<String, String> htblColNameMax2 = new Hashtable<String, String>();
        htblColNameMax2.put("SaleID", "10000");
        htblColNameMax2.put("SaleDate", "31.12.2030");
        htblColNameMax2.put("ProductID", "1000");
        htblColNameMax2.put("Quantity", "10000");
        htblColNameMax2.put("TotalAmount", "1000000000");
        
        Hashtable<String, String> htblForeignKeys2 = new Hashtable<String, String>();
        htblForeignKeys2.put("ProductID", "Product.ProductID");

        String[] computedCols2 = {"TotalAmount"};
        
        dbApp.createTable("Sales", "SaleID", htblColNameType2, htblColNameMin2, htblColNameMax2, htblForeignKeys2, computedCols2);
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

        htblColNameValue.put("ProductID", 800);
        htblColNameValue.put("ProductName", "Lenovo");
        htblColNameValue.put("ProductPrice", 96000.00);
        try {
            // dbApp.insertIntoTable("Product", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
        htblColNameValue.clear();
        htblColNameValue.put("ProductID", 400);
        htblColNameValue.put("ProductPrice", 20000.20);
        htblColNameValue.put("ProductName", "Acer");
        try {
            // dbApp.insertIntoTable("Product", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        htblColNameValue.clear();
        htblColNameValue.put("ProductID", new Integer( 1 ));
        htblColNameValue.put("ProductName", new String("TV" ) );
        htblColNameValue.put("ProductPrice", new Double( 499 ) );
        try {
            // dbApp.insertIntoTable("Product", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        htblColNameValue.clear( );
        htblColNameValue.put("ProductID", new Integer( 2 ));
        htblColNameValue.put("ProductName", new String("Mobile Phone" ) );
        htblColNameValue.put("ProductPrice", new Double( 299 ) );
        try {
            // dbApp.insertIntoTable("Product", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        htblColNameValue.clear( );
        htblColNameValue.put("ProductID", new Integer( 3 ));
        htblColNameValue.put("ProductName", new String("Power Bank" ) );
        htblColNameValue.put("ProductPrice", new Double( 15.5 ) );
        try {
            dbApp.insertIntoTable("Product", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
        
        // Insert into table Sales
        htblColNameValue.clear( );
        htblColNameValue.put("SaleID", new Integer( 1 ));
        htblColNameValue.put("ProductID", new Integer( 2 ) ); // Mobile Phone
        htblColNameValue.put("SaleDate", new Date(2012-1900, 1, 1));
        htblColNameValue.put("Quantity", 1200);
        try {
            // dbApp.insertIntoTable("Sales", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        
        htblColNameValue.clear();
        htblColNameValue.put("SaleID", 123);
        htblColNameValue.put("SaleDate", new Date(2002-1900, 1, 1));
        htblColNameValue.put("ProductID", 523);
        // htblColNameValue.put("TotalAmount", 96000.00);
        htblColNameValue.put("Quantity", 5000);
        try {
            // dbApp.insertIntoTable("Sales", htblColNameValue);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void updateTable() throws DBAppException {
        Hashtable<String,Object> htblColNameValue = new Hashtable<String, Object>();

        // htblColNameValue.put("ProductID", 200);
        htblColNameValue.put("ProductPrice", 80000.2);
        //  htblColNameValue.put("ProductPrice", 1000.00);

        dbApp.updateTable("Product", "100", htblColNameValue);
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

    public void selectFromTable() throws DBAppException {
        
        SQLTerm sqlTerm = new SQLTerm("Product", "ProductID", "<", 200);
        SQLTerm sqlTerm2 = new SQLTerm("Product", "ProductPrice", ">", 75000.00);
        SQLTerm sqlTerm3 = new SQLTerm("Product", "ProductName", "=", "Lenovo");
        
        SQLTerm[] sqlTerms = new SQLTerm[] {sqlTerm, sqlTerm2, sqlTerm3};
        String[] strArrOperators = new String[] {"AND", "OR"};

        Iterator<String> i = dbApp.selectFromTable(sqlTerms, strArrOperators);
        
        // Print out select results
        while (i.hasNext()) {
            System.out.println(i.next());
        }
    }
}
