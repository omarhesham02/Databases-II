package src.classes;

import java.io.*;
import java.util.*;
import src.DBApp;
import src.exceptions.DBAppException;

public class Page {
    final static int N = DBApp.N;

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
            System.err.println("Failed to load page " + intPageNum + " for table " + parentTable.getName());
        }
    }

    public boolean isFull() {
        return arrTuples.size() == N;
    }

    /**
     * Following method inserts one row only.
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public void insertIntoPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        // Ensure page has space
        if (this.isFull()) return;

        // Insert into place based on clustering key
        int arrSize = arrTuples.size();
        if (arrSize < 1) {
            arrTuples.add(stringify(htblColNameValue));
            isUpdated = true;
            return;
        }


        String colNameClusteringKey = parentTable.getClusteringKey();
        for (int i = 0; i < arrSize; i++) {
            String curr = arrTuples.get(i).get(colNameClusteringKey);
            Object insert = htblColNameValue.get(colNameClusteringKey);

            int compare = Functions.cmpObj(curr, insert, parentTable.getColType(colNameClusteringKey));

            // Insert here
            if (compare == 1) {
                Hashtable<String, String> tempTuple = stringify(htblColNameValue);

                for (int j = i; j < arrSize; j++) {
                    Hashtable<String, String> next = arrTuples.get(j);
                    arrTuples.set(j, tempTuple);
                    tempTuple = next;
                }
                arrTuples.add(tempTuple);
                isUpdated = true;
                return;
            }
        }

        // If reached end of file and not entered, then it must be the greatest
        if (!isUpdated) {
            arrTuples.add(stringify(htblColNameValue));
            isUpdated = true;
            return;
        }
        // TODO: Update grid index
    }

    public void updatePage (String strClusteringKey, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {  

                    // Iterate over the tuples of the page looking for a tuple with the clustering key value 
                    try {
                    Enumeration<String> colName = htblColNameValue.keys();
                    for (Hashtable<String, String> tuple : arrTuples) {
                        if (tuple.get(strClusteringKey).equals(strClusteringKeyValue)) {
                            while (colName.hasMoreElements()) {
                                String nextCol = colName.nextElement();
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

    private Hashtable<String, String> stringify(Hashtable<String, Object> tempTuple) {
        Hashtable<String, String> result = new Hashtable<String, String>();

        // Iterate over the given columns
        Enumeration<String> keys = tempTuple.keys();
        while (keys.hasMoreElements()) {
            String colName = keys.nextElement();
            result.put(colName, "" + tempTuple.get(colName));
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
}
