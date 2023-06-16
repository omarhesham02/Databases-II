package src;
import java.io.*;
import java.util.*;
import src.classes.*;
import src.exceptions.DBAppException;

public class DBApp  {
    public static final Properties Config = new Properties();

    private Table tblCurrent;

    public static void main(String[] args) throws DBAppException {
        Tests test = new Tests();
        // test.createTable();
        test.insertTable();
        // test.deleteFromTable();
        // test.createIndex();
        // test.selectFromTable();
    }

    public DBApp() {
        try {
            Config.load(new FileInputStream("./src/DBApp.config"));
            System.out.println(Config);
        } catch (IOException e) {
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
        final File TABLE_DIR = new File("./src/tables/" + strTableName);
        
        // If table already exist throw error, else create folder
        if (TABLE_DIR.exists()) {
            throw new DBAppException("Table " + strTableName + " already exists!");
        }
        TABLE_DIR.mkdirs();
        
        // Open metadata file
        FileWriter meta;
        try {
            meta = new FileWriter("./src/metadata.csv", true);
            
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

        loadTable(strTableName);
        
        // TODO: Implement grid index on given 2 columns
        GridIndex index = new GridIndex(tblCurrent, strarrColName[0], strarrColName[1]);
    }

    /**
     * Following method inserts one row only. 
     * @param strTableName
     * @param htblColNameValue must include a value for the primary key.
     * @throws DBAppException
     */
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        loadTable(strTableName);

        System.out.println("Inserting into table " + strTableName);
        tblCurrent.insertIntoTable(htblColNameValue);
    }
    
    /**
     * Following method updates one row only
     * @param strTableName
     * @param strClusteringKeyValue is the value to look for to find the row to update.
     * @param htblColNameValue holds the key and new value, and will not include clustering key as column name
     * @throws DBAppException
     */
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException {
        loadTable(strTableName);

        tblCurrent.updateTable(strTableName, strClusteringKeyValue, htblColNameValue);
    }


    /**
     * Following method could be used to delete one or more rows.
     * @param strTableName
     * @param htblColNameValue holds the key and value. This will be used in search to identify which rows/tuples to delete. Enteries are ANDED together
     * @throws DBAppException
     */
    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        loadTable(strTableName);

        tblCurrent.deleteFromTable(htblColNameValue); 
    }

    /**
     * 
     * @param arrSQLTerms
     * @param strarrOperators
     * @return results
     * @throws DBAppException
     */
    public Iterator<String> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        
        SQLTerm firstTerm = arrSQLTerms[0];
        loadTable(firstTerm.getTableName());

        ArrayList<String> result = new ArrayList<String>();
        // Iterate over the table

        TableScanner ts = new TableScanner(tblCurrent);

        while (ts.hasNext()) {
            Hashtable<String, String> currTuple = ts.next();
        
            // Iterate over the SQLTerms 

            for (int i = 0; i < arrSQLTerms.length - 1; i++) {
                SQLTerm currTerm = arrSQLTerms[i];
                SQLTerm nextTerm = arrSQLTerms[i + 1];

                String currValue = currTerm.getValue();
                String nextValue = nextTerm.getValue(); 
                // Check if the current tuple satisfies the current value and the next tuple satisfies the next value
                boolean currTermSatisfies = currTerm.evaluate(currTuple.get(currTerm.getColumnName()), tblCurrent.getColType(currTerm.getColumnName()));
                boolean nextTermSatisfies = nextTerm.evaluate(currTuple.get(nextTerm.getColumnName()), tblCurrent.getColType(nextTerm.getColumnName()));

                // Depending on the operator, check if the tuple should be added to the result
                String operator = strarrOperators[i];


                switch (operator) {
                    case "AND": 
                        if (currTermSatisfies && nextTermSatisfies) {
                            // Add the tuple to the result
                            result.add(ts.buildTuple(currTuple));
                        }
                        break;
                    
                    case "OR":
                        if (currTermSatisfies || nextTermSatisfies) {
                            // Add the tuple to the result
                            result.add(ts.buildTuple(currTuple));
                        }
                        break;

                    default:
                        throw new DBAppException("Invalid Operator " + operator);
                }

             }
        }
        


        for (String s : result) {
            System.out.println(s);
        }

        return result.iterator();
        
    }

    private void loadTable(String strTableName) throws DBAppException{
        if (this.tblCurrent != null && this.tblCurrent.getName().equals(strTableName)) {
            return;
        }

        this.tblCurrent = new Table(strTableName);
    }
}