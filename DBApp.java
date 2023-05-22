import exceptions.DBAppException;

class DBApp {
    
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

    }

    /**
     * Following method creates a grid index
     * If two column names are passed, create a grid index.
     * @param strTableName
     * @param strarrColName
     * @throws DBAppException when only one or more than 2 column names are passed
     */
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

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

    }

}