package src.classes;

import src.exceptions.DBAppException;

public class SQLTerm {
    public String strTableName;
    public String strColumnName;
    public String strOperator;
    public Object objValue;

    public SQLTerm (String strTableName,String strColumnName,String strOperator,Object objValue) {
        this.strTableName = strTableName;
        this.strColumnName = strColumnName;
        this.strOperator = strOperator;
        this.objValue = objValue;
    }


    public SQLTerm() {

    }

    
    public String getTableName() {
        return this.strTableName;
    }

    public String getColumnName() {
        return this.strColumnName;
    }

    public String getValue() {
        return this.objValue.toString();
    }

    public Boolean evaluate(String strValueToEval, String strColType) throws DBAppException {
        Functions.checkType(objValue, strColType);
        
        int comparator = Functions.cmpObj(strValueToEval, objValue, strColType);

        // Compare
        switch(this.strOperator) {
            case ">": {
                return comparator == 1;
            }
            case ">=": {
                return comparator > -1;
            }
            case "<": {
                return comparator == -1;
            }
            case "<=": {
                return comparator < 1;
            }
            case "=": {
                return comparator == 0;
            }
            default: {
                throw new DBAppException("Invalid SQLTerm Operator!");
            }
        }
    }
}
