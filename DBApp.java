import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import exceptions.DBAppException;
import classes.*;

public class DBApp  {
    public static final int N = 200;

    public static void main(String[] args) {
        DBApp dbApp = new DBApp();
        
        // Create table
        // try {
        //     Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        //     htblColNameType.put("ProductID", "java.lang.Integer");
        //     htblColNameType.put("ProductName", "java.lang.String");
        //     htblColNameType.put("ProductPrice", "java.lang.Double");
            
        //     Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        //     htblColNameMin.put("ProductID", "0");
        //     htblColNameMin.put("ProductName", "A");
        //     htblColNameMin.put("ProductPrice", "0");
            
        //     Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        //     htblColNameMax.put("ProductID", "1000");
        //     htblColNameMax.put("ProductName", "ZZZZZZZZZZZ");
        //     htblColNameMax.put("ProductPrice", "100000");
            
        //     Hashtable<String, String> htblForeignKeys = new Hashtable<String, String>();
            
        //     String[] computedCols = {};
            
        //     dbApp.createTable("Product", " ProductID ", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys, computedCols);
        // } catch (DBAppException e) {
        //     System.err.println("Can't Create Table.");
        //     e.printStackTrace();
        // }

        // Test Table class
        try {
            Table tbl = new Table("Product");
            String [] colNames = tbl.getColNames();

            for (String colName : colNames) {
                System.out.println(colName);
            }
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Following method creates one table only
     * @param strTableName
     * @param strClusteringKeyColumnname of the column that will be the primary key and the clustering column as well. The data type of that column will be passed in htblColNameType
     * @param htblColNameType will have the column name as key and the data type as value
     * @param htblColNameMin for passing minimum column values  Key is the name of the column
     * @param htblColNameMax for passing maximum column values  Key is the name of the column
     * @param htblForeignKeys for specifying which column is a foreign key the key is the ColumnName to set as foreign key, value is a string specifying the referenced key in format “TableName.ColumnName”
     * @param computedCols is an array of Strings that has the column names that their value is computed
     * @throws DBAppException
     */
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax, Hashtable<String,String> htblForeignKeys, String[] computedCols ) throws DBAppException {
        // Create table folder
        final File TABLE_DIR = new File("./tables/" + strTableName);
        
        // If table already exist throw error, else create folder
        if (TABLE_DIR.exists()) {
            throw new DBAppException("Table " + strTableName + " already exists!");
        }
        TABLE_DIR.mkdirs();
        
        // Open metadata file
        FileWriter meta;
        try {
            meta = new FileWriter("metadata.csv", true);
            
            // Enumerating the elements of the hashtable
            Enumeration<String> keys = htblColNameType.keys();
            
            // Iterate over the given columns
            while (keys.hasMoreElements()) {
                String colName = keys.nextElement();
                
                // Get attribute details based on the column name
                String attType = htblColNameType.get(colName);
            String attMax = htblColNameMax.get(colName);
            String attMin = htblColNameMin.get(colName);
            
            // Check if current column is cluster key
            boolean isClusterKey = strClusteringKeyColumn.equals(colName);
            
            // Initialize indexed to null
            String indexName = "null";
            String indexType = "null";
            
            // Check if foreign key and calculate values
            boolean isForeignKey = htblForeignKeys.containsKey(colName);
            String foreignTable = "null";
            String foreignColumn = "null";
            if (isForeignKey) {
                String[] data = htblForeignKeys.get(colName).split(".");
                foreignTable = data[0];
                foreignColumn = data[1];
            }

            // Check if this is a computed column
            boolean isComputed = false;
            for (String curr : computedCols) {
                if (curr.equals(colName)) {
                    isComputed = true;
                    break;
                }
            }
            
            // TableName, ColumnName, ColumnType, ClusteringKey, IndexName, IndexType, min, max, ForeignKey, ForeignTableName, ForeignColumnName, Compute
            String row = strTableName + "," + colName + "," + attType + "," + isClusterKey + "," + indexName + "," + indexType + "," + attMin + "," + attMax + "," + isForeignKey + "," + foreignTable + "," + foreignColumn + "," + isComputed;
            
            // Append this column to the metadata
            meta.append(row + "\n");
        }
        
        // Close metadata file
        meta.flush();
        meta.close();
    } catch (IOException e) {
        throw new DBAppException(e.getMessage());
    }
}

    /**
     * Following method creates a grid index
     * If two column names are passed, create a grid index.
     * @param strTableName
     * @param strarrColName
     * @throws DBAppException when only one or more than 2 column names are passed
     */
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        // If strarrColName not length 2 return
        if (strarrColName.length != 2) {
            throw new DBAppException("Can only create a grid index on two columns");
        }

        // Set path to the table's directory
        final File TABLE_DIR = new File("./tables/" + strTableName);
        
        // TODO: Implement grid index on given 2 columns
        
    }

    /**
     * Following method inserts one row only. 
     * @param strTableName
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        // TODO: Ensure primary key has a value

    }
    
    /**
     * Following method updates one row only
     * @param strTableName
     * @param strClusteringKeyValue is the value to look for to find the row to update.
     * @param htblColNameValue holds the key and new value, and will not include clustering key as column name
     * @throws DBAppException
     */
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException {

    }

    /**
     * Following method could be used to delete one or more rows.
     * @param strTableName
     * @param htblColNameValue holds the key and value. This will be used in search to identify which rows/tuples to delete. Enteries are ANDED together
     * @throws DBAppException
     */
    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {

    }

    /**
     * 
     * @param arrSQLTerms
     * @param strarrOperators
     * @return
     * @throws DBAppException
     */
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        ArrayList<String> results = new ArrayList<String>();
        
        return results.iterator();
    }

}