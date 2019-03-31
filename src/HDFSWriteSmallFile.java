import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HDFSWriteSmallFile {
    public static void main(String[] args){
        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
        	for (int i = 1; i <= (131072); ++i) {
                String fileName = "./smallFiles/" + i;
                Path file = new Path(fileName);
                createFile(fs, file);
                if (i % (1<<10) == 0) {
                	System.out.println("Created " + i);
                }
        	}
            fs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void createFile(FileSystem fs, Path file) throws IOException{
        byte[] buff = "qwq".getBytes();
        FSDataOutputStream os =fs.create(file);
        os.write(buff,0,buff.length);
//        System.out.println("Create:"+file.getName());
        os.close();
    }
}