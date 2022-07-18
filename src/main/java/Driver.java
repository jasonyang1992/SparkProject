import Processor.ApplicationProcessor;
import Processor.EmployeeProcessor;
import Processor.PayCheckProcessor;

public class Driver {

    public static void main (String[] args){

        try {
            validateArguments(args);
            ApplicationProcessor applicationProcessor = null;
            if (args[0].equalsIgnoreCase("pay")){
                applicationProcessor = new PayCheckProcessor();
            } else if (args[0].equalsIgnoreCase("employees")){
                applicationProcessor = new EmployeeProcessor();
            }
            if(applicationProcessor != null) {
                applicationProcessor.startProcessor();
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static void validateArguments(String[] args) throws Exception{
        if(args.length == 1){
            if(args[0].equalsIgnoreCase("pay")){
                System.out.println(args[0] + " is a validate argument");
            } else if (args[0].equalsIgnoreCase("employees")){
                System.out.println(args[0] + " is a validate argument");
            } else {
                throw  new Exception("This is an invalid argument " + args[0]);
            }
        } else {
            throw new Exception("This is an invalid argument/s " + args[0]);
        }
    }
}
