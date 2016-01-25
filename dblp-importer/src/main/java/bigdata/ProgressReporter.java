package bigdata;

public class ProgressReporter {
    public void report(long counter) {
        if(counter % 200000 == 0) {
            System.out.format("%n%,11d -", counter);
            System.out.flush();
        } else if(counter % 3125 == 0) {
            System.out.print('-');
            System.out.flush();
        }
    }
}
