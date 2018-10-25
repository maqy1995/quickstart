package guowenpeng;

public class Stream_batch_twothread {
    public static void main(String[] args) throws InterruptedException {
        String batch_input_path=args[0];
        String batch_ouput_path=args[1];
        String stream_input_path=args[2];
        String stream_ouput_path=args[3];
        Runnable task1=new stream(batch_input_path,batch_ouput_path);
        Runnable task2=new batch(stream_input_path,stream_ouput_path);
        Thread thread1=new Thread(task1);
        Thread thread2=new Thread(task2);
        thread1.start();
        thread1.join();
        thread2.start();
        thread2.join();
        System.out.println("main end");
    }
}
