import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PerformanceTest {
	private static void SequentialReadSpeedTest() throws IOException {
		String path1 = "./GetData_NORMAL", path2 = "./GetData_PACKAGE";
		//Test normal GET
		FileSolver.deletefile(path1);
		File file1 = new File(path1);
		file1.mkdir();
		FileSolver.deletefile(path2);
		File file2 = new File(path2);
		file2.mkdir();

        
		//Test Package GET
		long timer2 = System.nanoTime();
		HDFSPackage pack = new HDFSPackage("medicalImgs");
		pack.getFilesFromPackage(path2);
        pack.close();
        timer2 = System.nanoTime() - timer2;
        

        //Test Normal GET
		long timer1 = System.nanoTime();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://Master:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        fs.copyToLocalFile(false, new Path("./subset"), new Path(path1));
        fs.close();
        timer1 = System.nanoTime() - timer1;
        
        
        
        System.out.println("SequentialReadSpeedTest:");
        System.out.println("NORMAL_TIMER: " + timer1 + "; PACKAGE TIMER: " + timer2);
	}
	public static void RandomReadSpeedTest(int counter) throws FileNotFoundException, IOException {
		String mPath = "./../non_cancer_subset00";
		File file = new File(mPath);
        String[] fileNames = file.list();
        
        List<String> choosedFiles = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < counter; ++i) {
        	choosedFiles.add(fileNames[rand.nextInt(fileNames.length)]);
        }
        
		String path1 = "./GetData_NORMAL", path2 = "./GetData_PACKAGE";
		//Test normal GET
		FileSolver.deletefile(path1);
		File file1 = new File(path1);
		file1.mkdir();
		FileSolver.deletefile(path2);
		File file2 = new File(path2);
		file2.mkdir();

        
        //Test Normal GET
		long timer1 = 0;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://Master:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < counter; ++i) {
    		long timer = System.nanoTime();
            fs.copyToLocalFile(false, new Path("./subset/"+choosedFiles.get(i)), new Path(path1+"/"+i+".tiff"));
            timer1 += System.nanoTime() - timer;
        }
        fs.close();
//        timer1 = System.nanoTime() - timer1;
        
        
		//Test Package GET
		long timer2 = 0;
		HDFSPackage pack = new HDFSPackage("medicalImgs");
        for (int i = 0; i < counter; ++i) {
    		long timer = System.nanoTime();
        	pack.getFileFromPackage(choosedFiles.get(i), path2+"/"+i+".tiff");
            timer2 += System.nanoTime() - timer;
        }
        pack.close();
//        timer2 = System.nanoTime() - timer2;

        
        System.out.println("RandomReadSpeedTest:");
        System.out.println("NORMAL_TIMER: " + timer1 + "; PACKAGE TIMER: " + timer2);
	}
	public static void main(String[] args) throws IOException {
//		SequentialReadSpeedTest();
//		SequentialReadSpeedTest();
		RandomReadSpeedTest(10);
	}
}
