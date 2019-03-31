import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HDFSWriteRead {
    public static void main(String[] args){
        try{
            String fileName = "test";
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(fileName);
            if(fs.exists(file)){
                System.out.println("File exists");
            }else{
                System.out.println("File does not exist");
                createFile(fs,file);
            }
            readFile(fs,file);
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void createFile(FileSystem fs, Path file) throws IOException{
        byte[] buff = "Hello World".getBytes();
        FSDataOutputStream os =fs.create(file);
        os.write(buff,0,buff.length);
        System.out.println("Create:"+file.getName());
        os.close();
    }
    public static void readFile(FileSystem fs, Path file) throws IOException{
            FSDataInputStream in = fs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(in));
            String content = d.readLine();
            System.out.println(content);
            d.close();
            in.close();
    }
}