import java.io.FileWriter;
import java.io.IOException;

public class csvtest {

	public static void main(String[] args) {
		
		try {
			//create file writer with parameter true in order to maintain values
			
					//create or edit the csv has same steps
		        FileWriter fw = new FileWriter("C:\\Users\\mzaka\\Desktop\\Documents\\literallyme.csv",true);


			
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
