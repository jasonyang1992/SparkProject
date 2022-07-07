import Processor.ApplicationProcessor;
import Processor.PayCheckProcessor;

public class Driver {

    public static void main (String[] args){
        ApplicationProcessor applicationProcessor = new PayCheckProcessor();
        applicationProcessor.startProcessor();
    }
}
