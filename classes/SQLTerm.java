package classes;

import exceptions.DBAppException;

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

    public Boolean evaluate(Object objValueToEval) throws DBAppException {
        // TODO: Type cast both objects

        // Compare
        switch(this.strOperator) {
            case ">": {
                
                break;
            }
            case ">=": {

                break;
            }
            case "<": {

                break;
            }
            case "<=": {
                
                break;
            }
            case "=": {
                return objValueToEval.equals(this.objValue);
            }
            default: {
                throw new DBAppException("Invalid SQLTerm Operator!");
            }
        }

        return false;
    }
}
