import java.io.FileWriter;
import java.io.IOException;

public class csvtest {

	public static void main(String[] args) {
		
		
		try {
			//create file writer with parameter true in order to maintain values
			FileWriter fw = new FileWriter("Heroes.csv",true);
			
			//way 1 
            fw.append("Ryan Gosling,American Psycho\n");
			//way 2
            fw.append("Patrick Bateman");
            fw.append(",");
            fw.append("Drive");
            fw.append("\n");
            //then flush and close
            fw.flush();
            fw.close();
            
			System.out.println("succsess?????");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("errrororrrr");
		}
		
		
	}
}
