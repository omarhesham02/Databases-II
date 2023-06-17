package src.classes;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import src.DBApp;
import src.exceptions.DBAppException;

public class Page {
    final static int N = Integer.parseInt(DBApp.Config.getProperty("NUM_ROWS_PER_PAGE"));

    private int intPageNum;
    private Boolean isUpdated = false;
    private String clusterMin;
    private String clusterMax;
    private Table parentTable;
    private ArrayList<Hashtable<String, String>> arrTuples = new ArrayList<Hashtable<String, String>>();
    private File PAGE_PATH; 

    public Page(Table tbl, int intPageNum) {
        System.out.println("Fetching page " + intPageNum + " from table " + tbl.getName());
        this.parentTable = tbl;
        this.intPageNum = intPageNum;
        this.PAGE_PATH = new File("./src/tables/" + parentTable.getName() + "/" + intPageNum + ".csv");
        load();
    }

    public ArrayList<Hashtable<String, String>> getArrTuples() {
        return this.arrTuples;
    }

    /**
     * Load existing tuples into this page
     */
    public void load() {
        System.out.println("Loading page...");
        if (!this.PAGE_PATH.exists()) return;

        try {
            String line = "";
            BufferedReader pageFile = new BufferedReader(new FileReader(PAGE_PATH));
            
            while ((line = pageFile.readLine()) != null) {
                Hashtable<String, String> tuple = breakTuple(line);
                arrTuples.add(tuple);
            }

            clusterMin = arrTuples.get(0).get(parentTable.getClusteringKey());
            clusterMax = arrTuples.get(arrTuples.size() - 1).get(parentTable.getClusteringKey());

            pageFile.close();
        } catch (Exception e) {
            System.err.println("Failed to load page " + intPageNum + " for table " + parentTable.getName() + "\nCause: " + e.getMessage());
        }
    }

    public boolean isFull() {
        return arrTuples.size() >= N;
    }

    // Linear start for insert
    public Hashtable<String, String> insertIntoPage(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        return insertIntoPage(htblColNameValue, 0);
    }

    /**
     * Following method inserts one row only.
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public Hashtable<String, String> insertIntoPage(Hashtable<String,Object> htblColNameValue, int startIndex) throws DBAppException {
        // Insert into place based on clustering key
        int arrSize = arrTuples.size();
        if (arrSize < 1) {
            arrTuples.add(stringify(htblColNameValue, parentTable));
            isUpdated = true;
            return null;
        }

        // Iterate over all tuples in page starting from given index
        String colNameClusteringKey = parentTable.getClusteringKey();
        Hashtable<String, String> tempTuple = null;
        for (int i = startIndex; i < arrSize && !isUpdated; i++) {
            String curr = arrTuples.get(i).get(colNameClusteringKey);
            Object insert = htblColNameValue.get(colNameClusteringKey);

            int compare = Functions.cmpObj(curr, insert, parentTable.getColType(colNameClusteringKey));

            // If equal, throw error
            if (compare == 0) {
                throw new DBAppException("Clustering index " + curr + " already exists.");
            }

            // Insert here
            if (compare == 1) {
                tempTuple = stringify(htblColNameValue, parentTable);

                for (int j = i; j < arrSize; j++) {
                    Hashtable<String, String> next = arrTuples.get(j);
                    arrTuples.set(j, tempTuple);
                    tempTuple = next;
                }
                isUpdated = true;

                if (!isFull()) {
                    arrTuples.add(tempTuple);
                    tempTuple = null;
                } 
            }
        }

        // If reached end of file and not entered, then it must be the greatest
        if (!isUpdated) {
            arrTuples.add(stringify(htblColNameValue, parentTable));
            isUpdated = true;
            return null;
        }

        return tempTuple;
    }

    public void updateTuple(int index, Hashtable<String, Object> newValues) {
        Hashtable<String, String> currentTuple = arrTuples.get(index);
        Hashtable<String, String> convertedValues = stringify(newValues, parentTable);

        Enumeration<String> colNames = convertedValues.keys();
        while (colNames.hasMoreElements()) {
            String colName = colNames.nextElement();
            currentTuple.replace(colName, convertedValues.get(colName));
        }
        isUpdated = true;
    }

    public void updatePage (String strClusteringKey, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {  

        // Iterate over the tuples of the page looking for a tuple with the clustering key value 
        try {
            Enumeration<String> colNames = htblColNameValue.keys();
            for (Hashtable<String, String> tuple : arrTuples) {
                if (tuple.get(strClusteringKey).equals(strClusteringKeyValue)) {
                    while (colNames.hasMoreElements()) {
                        String nextCol = colNames.nextElement();
                        String colType = parentTable.getColType(nextCol);
                        Functions.checkType(htblColNameValue.get(nextCol), colType);                                        
                        
                        String oldTupleValue = (String) tuple.get(nextCol);
                        Object newTupleValue = htblColNameValue.get(nextCol);
                        
                        if (Functions.cmpObj(oldTupleValue, newTupleValue, colType) != 0) {
                            tuple.put(nextCol, "" + newTupleValue); 
                            this.isUpdated = true;
                            break;

                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteFromPage(Hashtable<String,Object> htblColNameValue) {
        // Iterate over the tuples of the page looking for a tuple with an attribute and its corresponding value that exists in htblColNameValue
        // If found, delete the tuple
        try {
            Enumeration<String> colName = htblColNameValue.keys();
                 // Check if this tuple has any attribute with a value that exists in htblColNameValue
                // -> Iterate over htblColNameValue and check if there are any attributes from it contained in the tuple. If so, check if they have the same value

                int countDeleted = 0;
                
                for (int i = 0; i < arrTuples.size() - countDeleted; i++) {
                    Hashtable<String, String> tuple = arrTuples.get(i);
                    String nextCol = colName.nextElement();
                    String colType = parentTable.getColType(nextCol);
                                                       
                    // Check if the tuple does not have this attribute
                    if (!tuple.containsKey(nextCol)) 
                        continue;

                    // If it does, check if the value of the attribute in the tuple is the same as the value in htblColNameValue
                    // If a value does not match, continue and move on to the next tuple
                    String tupleValue = tuple.get(nextCol);
                        if (Functions.cmpObj(tupleValue, htblColNameValue.get(nextCol), colType) != 0)  {
                            continue;
                        }

                    // Otherwise, delete the tuple and move on to the next tuple
                    arrTuples.remove(i);
                    countDeleted++;
                    i--;
                    this.isUpdated = true;

                    // if (Functions.cmpObj(tupleValue, deleteValue, colType) == 0) {
                        // arrTuples.remove(tuple);
                        // this.isUpdated = true;
                        // break;
                    }
                } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws DBAppException {
        if (!isUpdated) return;
        FileWriter fw;
        BufferedWriter bw;
        try {
            fw = new FileWriter(PAGE_PATH, false);
            bw = new BufferedWriter(fw);
            for (Hashtable<String, String> htbl: arrTuples) {
                bw.write(buildTuple(htbl));
                bw.newLine();
            }
            
            bw.close();
            // fw.flush();
            // fw.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBAppException("Error saving page " + intPageNum);
        }
    }

    private String buildTuple(Hashtable<String, String> htblColNameValue) {
        ArrayList<String> OrderedColumns = parentTable.getColNames();
        String[] temp = new String[OrderedColumns.size()];

        int i = 0;
        for (String s: OrderedColumns) {
            if (htblColNameValue.get(s) != null) {
            temp[i++] = htblColNameValue.get(s);
            } else {
                temp[i++] = "";
            }
        }

        return String.join(",", temp);
    }

    private Hashtable<String, String> breakTuple(String line) {
        Hashtable<String, String> tuple = new Hashtable<String, String>();
        ArrayList<String> OrderedColumns = parentTable.getColNames();
        String[] temp = line.split(",");

        int i = 0;
        for (String s: OrderedColumns) {
            tuple.put(s, temp[i++]);
        }

        return tuple;
    }

    public static Hashtable<String, String> stringify(Hashtable<String, Object> tempTuple, Table parentTable) {
        Hashtable<String, String> result = new Hashtable<String, String>();

        // Iterate over the given columns
        Enumeration<String> keys = tempTuple.keys();
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();

            if (tempTuple.get(colName) == null) {
                result.put(colName, null);
                continue;
            }

            String strCol = tempTuple.get(colName).toString();

            // If date, needs special formatting
            if (parentTable.getColType(colName).equals("java.util.Date")) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.YYYY");
                strCol = dateFormat.format(tempTuple.get(colName));
            }

            result.put(colName, strCol);
        }

        return result;
    }

    public Hashtable<String, String> getTuple(int tupleIndex) {
        return arrTuples.get(tupleIndex);
    }

    public int size() {
        return arrTuples.size();
    }

    public int getNum() {
        return intPageNum;
    }

    public String getMinCluster() {
        return clusterMin;
    }

    public String getMaxCluster() {
        return clusterMax;
    }

    /**
     * Find the index of a tuple with a given clustering key value.
     * @param clusterValue
     * @return index of tuple in this page, else {@code -1} if it doesn't exist 
     */
    public int findIndex(Object clusterValue) {
        // Iterate over tuples in this page
        for (int i = 0; i < arrTuples.size(); i++) {
            String clusterColName = parentTable.getClusteringKey();
            String clusterColType = parentTable.getColType(clusterColName);
            String currClusterVal = arrTuples.get(i).get(clusterColName);

            int comparator = Functions.cmpObj(currClusterVal, clusterValue, clusterColType);

            if (comparator == 1) {
                return -1;
            }

            if (comparator == 0) {
                return i;
            }
        }

        return -1;
    }

    public Hashtable<String, String> forceInsert(Hashtable<String, String> kickedOut) throws DBAppException {
        // Parse it
        Enumeration<String> colNames = kickedOut.keys();
        Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        while (colNames.hasMoreElements()) {
            String colName = colNames.nextElement();
            Object parsed = GridIndex.strToObj(kickedOut.get(colName), parentTable.getColType(colName));
            htblColNameValue.put(colName, parsed);
        }

        return insertIntoPage(htblColNameValue);
    }
}
