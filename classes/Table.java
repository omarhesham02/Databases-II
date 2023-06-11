package classes;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import exceptions.DBAppException;


public class Table {
    private String TableName;
    private Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
    private String ColNameClusteringKey = null;
    private Hashtable<String, String> htblColNameIndexName = new Hashtable<String, String>();
    private Hashtable<String, String> htblColNameIndexType = new Hashtable<String, String>();
    private Hashtable<String, Object> htblColNameMin = new Hashtable<String, Object>();
    private Hashtable<String, Object> htblColNameMax = new Hashtable<String, Object>();
    private Hashtable<String, Boolean> htblColNameForeignKey = new Hashtable<String, Boolean>();
    private Hashtable<String, String> htblColNameForeignTable = new Hashtable<String, String>();
    private Hashtable<String, String> htblColNameForeignColumnName = new Hashtable<String, String>();
    private Hashtable<String, Boolean> htblColNameComputed = new Hashtable<String, Boolean>();
    private File TABLE_DIR;

    public Table(String strTableName) throws DBAppException {
        this.TableName = strTableName;

        // If table doesn't already exist throw error
        TABLE_DIR = new File("./tables/" + strTableName);
        if (!TABLE_DIR.exists()) {
            throw new DBAppException("Cannot Create Table Class.\nTable " + strTableName + " doesn't exist!");
        }

        // Get table data from meta data
        String line = "";  
        String splitBy = ",";  
        try {
            // Iterate over rows in metadata
            BufferedReader br = new BufferedReader(new FileReader("metadata.csv"));  
            
            // Look for this table
            while ((line = br.readLine()) != null) {  
                String[] row = line.split(splitBy);

                // If not table, continue
                if (!row[0].equals(strTableName)) continue;
                String colName = row[1];

                htblColNameType.put(colName, row[2]);
                if (Boolean.parseBoolean(row[3])) { // If it is clustering key, reassign table's clustering key
                    ColNameClusteringKey = colName;
                }
                htblColNameIndexName.put(colName, row[4]);
                htblColNameIndexType.put(colName, row[5]);
                htblColNameMin.put(colName, row[6]);
                htblColNameMax.put(colName, row[7]);
                htblColNameForeignKey.put(colName, Boolean.parseBoolean(row[8]));
                htblColNameForeignTable.put(colName, row[9]);
                htblColNameForeignColumnName.put(colName, row[10]);
                htblColNameComputed.put(colName, Boolean.parseBoolean(row[11]));
            }

            br.close();
            
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }

    /**
     * Following method inserts one row only.
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public void insertIntoTable(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        // Ensure primary key has a value
        Object clusterValue = htblColNameValue.get(ColNameClusteringKey);
        if (clusterValue == null) {
            throw new DBAppException("Clustering key value cannot be null");
        }

        // TODO: Ensure proper column types passed
        // Check if foreign key it exists in foreign table
        Enumeration<String> keys = htblColNameType.keys();
        
        // Iterate over the given columns
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();
            String colType = this.htblColNameType.get(colName);
            
            // Check if foreign key exists in foreign table
            Boolean foreignKey = this.htblColNameForeignKey.get(colName);
            if (foreignKey) {
                String foreignTable = this.htblColNameForeignTable.get(colName);
                Object foreignValue = htblColNameValue.get(colName);

                Table tempForeign = new Table(foreignTable);
                Boolean exists = tempForeign.clusterKeyExists(foreignValue);

                if (!exists) {
                    throw new DBAppException("Cannot insert.\nForeign key " + colName + " with value " + foreignValue + " does not exist in " + foreignTable);
                }
            }

            // TODO: Check same type

            // TODO: Check Min Max
            // if (htblColNameValue.get(colName))
        }

        // Call insert in page
        
        // TODO: Else linearly do it :)
        final File[] PAGE_FILES = TABLE_DIR.listFiles();
        for (File curr: PAGE_FILES) {
            try {
                curr.getAbsolutePath().split("/");
                Page p = new Page(TableName, 0);
                
                // If page already full go to next page
                if (p.isFull()) continue;
                
                p.insertIntoPage(htblColNameValue);
            } catch (Exception e) {
                // TODO: handle exception
            }
        } 
    }

    /**
     * Following method could be used to delete one or more rows.
     * @param htblColNameValue holds the key and value. This will be used in search to identify which rows/tuples to delete. Enteries are ANDED together
     * @throws DBAppException
     */
    public void deleteFromTable(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        // Ensure delete constraint is of same data type
        Enumeration<String> keys = htblColNameValue.keys();
        
        // Iterate over the given columns
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();
            String colType = this.htblColNameType.get(colName);

            // Check same type
        }
        
        // TODO: Check if at least column with index

        // TODO: Else linearly do it :)
        final File[] PAGE_FILES = TABLE_DIR.listFiles();
        for (File curr: PAGE_FILES) {
            
        }   
    }
    // TODO!!!!
    public boolean clusterKeyExists(Object x) {
        // TODO: If indexed use index

        // Else search linearly
        return true;

    }
    
    public String[] getColNames() {
        Object[] arrObj = htblColNameType.keySet().toArray();

        return Arrays.asList(arrObj).toArray(new String[arrObj.length]);
    }
}