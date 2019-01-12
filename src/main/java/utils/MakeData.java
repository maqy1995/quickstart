package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MakeData {

    public static void makeA9B1() throws IOException {
        String path = "F:\\a8b2c5d1e1f1g1h1j1_100K";
        File file = new File(path);
        if(!file.exists()){
            file.getParentFile().mkdirs();
        }
        file.createNewFile();


        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);
        int i=0;
        long length=100*1024;//单位是byte
        System.out.println(length);
        while(file.length()<length){
//            if(i%100000==0) {
//                System.out.println("file.length:" + file.length());
//            }
            bw.write("a a a b c d e f g h a a b a a a c c c c i j k l m n \n");
            bw.flush();
            i++;
        }
        bw.close();
        fw.close();
        System.out.println("总行数："+i);
    }

    public static void main(String[] args) throws IOException {
        makeA9B1();
    }
}

