package src.classes;
import java.io.*;
import java.util.*;

import src.exceptions.DBAppException;

public class Table {
    private String strTableName;
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
    private File TABLE_DIR;
    private int numPages;

    public Table(String strTableName) throws DBAppException {
        System.out.println("Loading table " + strTableName);

        this.strTableName = strTableName;

        // If table doesn't already exist throw error
        TABLE_DIR = new File("./src/tables/" + strTableName);
        if (!TABLE_DIR.exists()) {
            throw new DBAppException("Cannot Create Table Class.\nTable " + strTableName + " doesn't exist!");
        }

        final File[] PAGE_FILES = TABLE_DIR.listFiles();
        this.numPages = PAGE_FILES.length;
        loadMetadata();
    }

    public void loadMetadata() {
        System.out.println("Loading table " + this.strTableName + "'s properties from metadata...");

        this.htblColNameType.clear();
        this.htblColNameIndexName.clear();
        this.htblColNameIndexType.clear();
        this.htblColNameMin.clear();
        this.htblColNameMax.clear();
        this.htblColNameForeignKey.clear();
        this.htblColNameForeignTable.clear();
        this.htblColNameForeignColumnName.clear();
        this.htblColNameComputed.clear();
        
        // Get table data from meta data
        String line = "";  
        String splitBy = ",";  
        try {
            // Iterate over rows in metadata
            BufferedReader br = new BufferedReader(new FileReader("./src/metadata.csv"));  
            
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
        Enumeration<String> keys = htblColNameType.keys();
        
        // Iterate over the given columns
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();
            String colType = this.htblColNameType.get(colName);
            Object objValue = htblColNameValue.get(colName);
            
            // Check if foreign key exists in foreign table
            Boolean foreignKey = this.htblColNameForeignKey.get(colName);
            if (foreignKey) {
                String foreignTable = this.htblColNameForeignTable.get(colName);
                Object foreignValue = objValue;

                Table tempForeign = new Table(foreignTable);
                Boolean exists = tempForeign.clusterKeyExists(foreignValue);

                if (!exists) {
                    throw new DBAppException("Cannot insert.\nForeign key " + colName + " with value " + foreignValue + " does not exist in " + foreignTable);
                }
            }

            // Check same type
            if (objValue == null) continue;

            Functions.checkType(objValue, colType);

            // Check Min Max
            int cmpValueMax = Functions.cmpObj((String) htblColNameMax.get(colName), objValue, colType);
            int cmpValueMin = Functions.cmpObj((String) htblColNameMin.get(colName), objValue, colType);
            if (cmpValueMax < 0 || cmpValueMin > 0) {
                throw new DBAppException("Cannot insert.\nValue passed was out of bounds for column " + colType);
            }
        }

        // Call insert in page
        if (!htblColNameIndexName.get(ColNameClusteringKey).equals("null")) {
            System.out.println("Index found for " + ColNameClusteringKey + " while inserting in " + strTableName);

            // TODO
        }
        
        // TODO: Else linearly do it :)
        final File[] PAGE_FILES = TABLE_DIR.listFiles();
        int i = 0;
        Boolean inserted = false;
        for (File curr: PAGE_FILES) {
            Page p = new Page(this, i);
        
            i++;
            // If page already full and will not fit this index go to next page
            int comparator = Functions.cmpObj(p.getMaxCluster(), htblColNameValue.get(ColNameClusteringKey), htblColNameType.get(ColNameClusteringKey));
            if (p.isFull() && comparator < 1) continue;
            
            p.insertIntoPage(htblColNameValue);
            p.close();
        }
        
        // If not inserted, make new page on num i
        if (!inserted) {
            Page p = new Page(this, i);
            p.insertIntoPage(htblColNameValue);
            p.close();
        }

    }

 /**
     * Following method updates one row only
     * @param strTableName
     * @param strClusteringKeyValue is the value to look for to find the row to update.
     * @param htblColNameValue holds the key and new value, and will not include clustering key as column name
     * @throws DBAppException
     */
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {
            // TODO: Use index to find the row to update if strClusteringKeyValue has an index

            Table tb = new Table(strTableName);
            
            String strClusteringKey = tb.getClusteringKey();

            // Scan all page files linearly to update all matching records
            final File[] PAGE_FILES = TABLE_DIR.listFiles();

            int i = 0;
            for (File curr: PAGE_FILES) {
                try {
                    Page p = new Page(this, i++);
                    p.updatePage(strClusteringKey, strClusteringKeyValue, htblColNameValue);
                    p.close();
                } catch (Exception e) {
                    e.printStackTrace();
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
            Object objValue = htblColNameValue.get(colName);

            Functions.checkType(objValue, colType);
        }
        
        // TODO: Check if at least column with index

        // TODO: Else linearly do it :)
        final File[] PAGE_FILES = TABLE_DIR.listFiles();
        int i = 0;
        for (File curr: PAGE_FILES) {
            try {
                    Page p = new Page(this, i++);
                    p.deleteFromPage(htblColNameValue);
                    p.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }   
    }

    public boolean clusterKeyExists(Object x) {
        // TODO: If indexed use index

        // Else search linearly
        final File[] PAGE_FILES = TABLE_DIR.listFiles();
        int i = 0;
        for (File curr: PAGE_FILES) {
            Page p = new Page(this, i);
            i++;

            // If clustering key more than maximum of page, continue
            int comparatorMax = Functions.cmpObj(p.getMaxCluster(), x, htblColNameType.get(this.ColNameClusteringKey));
            if (comparatorMax == -1) {
                continue;
            }
            
            // If clustering key less than minimum of page, return false
            int comparatorMin = Functions.cmpObj(p.getMinCluster(), x, htblColNameType.get(this.ColNameClusteringKey));
            if (comparatorMin == 1) {
                return false;
            }
            
            // If clustering key is the max or min then it does exist
            if (comparatorMin == 0 || comparatorMax == 0) {
                return true;
            }

            // Look through this page
            return (p.findIndex(x) != -1);
        } 
        return false;
    }

    public ArrayList<String> getColNames() {
        ArrayList<String> arrList = new ArrayList<String>();

        // Get table data from meta data
        String line = "";  
        String splitBy = ",";  
        try {
            // Iterate over rows in metadata
            BufferedReader br = new BufferedReader(new FileReader("./src/metadata.csv"));  
            
            // Look for this table
            while ((line = br.readLine()) != null) {  
                String[] row = line.split(splitBy);

                // If not table, continue
                if (!row[0].equals(this.strTableName)) continue;
            
                arrList.add(row[1]);
            }

            br.close();
        } catch(Exception e) {
            System.err.println("Error Getting Column names.");
        }

        return arrList;
    }

    public String getName() {
        return this.strTableName;
    }

    public String getClusteringKey() {
        return this.ColNameClusteringKey;
    }

    public String getColType(String colName) {
        return htblColNameType.get(colName);
    }

    public Boolean colExists(String colName) {
        return (htblColNameType.get(colName) != null);
    }

    public String getColMin(String colName) {
        return htblColNameMin.get(colName);
    }

    public String getColMax(String colName) {
        return htblColNameMax.get(colName);
    }

    public int numPages() {
        return this.numPages;
    }

}