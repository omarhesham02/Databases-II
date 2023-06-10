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
    private Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
    private Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
    private Hashtable<String, Boolean> htblColNameForeignKey = new Hashtable<String, Boolean>();
    private Hashtable<String, String> htblColNameForeignTable = new Hashtable<String, String>();
    private Hashtable<String, String> htblColNameForeignColumnName = new Hashtable<String, String>();
    private Hashtable<String, Boolean> htblColNameComputed = new Hashtable<String, Boolean>();

    public Table(String strTableName) throws DBAppException {
        this.TableName = strTableName;

        // If table doesn't already exist throw error
        File TABLE_DIR = new File("./tables/" + strTableName);
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
    
    public String[] getColNames() {
        Object[] arrObj = htblColNameType.keySet().toArray();

        return Arrays.asList(arrObj).toArray(new String[arrObj.length]);
    }
}