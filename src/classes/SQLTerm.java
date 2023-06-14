package src.classes;

import src.exceptions.DBAppException;

public class SQLTerm {
    public String strTableName;
    public String strColumnName;
    public String strOperator;
    public String strColType;
    public Object objValue;

    public SQLTerm (String strTableName,String strColumnName,String strOperator,Object objValue) {
        this.strTableName = strTableName;
        this.strColumnName = strColumnName;
        this.strOperator = strOperator;
        this.objValue = objValue;
        try {
            this.strColType = new Table(strTableName).getColType(strColumnName);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }

    public SQLTerm() {

    }

    public Boolean evaluate(String strValueToEval) throws DBAppException {
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
