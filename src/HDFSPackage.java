import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

class IndexTable {
	List<String[]> table;
	FileSystem fs;
	Path path;
	int inFile;//the number of records saved in FS
    public void saveTable() throws IOException {
        FSDataOutputStream os = fs.append(path);
    	for (; inFile < table.size(); ++inFile) {
        	//append the new records into FS
    		String[] strs = table.get(inFile);
    		String record = String.join(" ", strs) + "\n";
            byte[] buff = record.getBytes();

            os.write(buff,0,buff.length);
    	}
        os.close();
    }
    IndexTable(FileSystem fs, Path path) throws IOException{
    	this.table = new ArrayList<>();
    	this.fs = fs;
    	this.path = path;
        FSDataInputStream in = fs.open(path);
        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        while (true) {
            String content = d.readLine();
            if (content == null) {
            	break;
            }
            String[] strs = content.split(" ");
            table.add(strs);
//	            String fileName = strs[0], fileSize = strs[1], fileOffset = strs[2]
//            System.out.println(content);
        }
        inFile = table.size();
        
        d.close();
        in.close();
    }
    public void addRecord(String fileName, long fileSize, long fileOffset) {
    	String[] strs = new String[]{fileName+"", String.valueOf(fileSize), String.valueOf(fileOffset)};
    	table.add(strs);
    }
    public long[] getRecord(String fileName) {
    	String[] strs = table.stream().filter(item -> item[0].equals(fileName)).findAny().orElse(null);
    	if (strs == null) {
    		System.out.println("Error: didn't find file " + fileName);
    		return null;
    	} else {
    		long fileSize = Long.valueOf(strs[1]);
    		long fileOffset = Long.valueOf(strs[2]);
    		long[] vals = new long[]{fileSize, fileOffset};
    		return vals;
    	}
    }
    public void showList() {
    	table.stream().forEach(item -> {
    		System.out.println(String.join(" ", item));
    	});
    }
    public List<String> getList() {
    	return table.stream().map(item -> item[0]).collect(Collectors.toList());
    }
}
public class HDFSPackage {
	private String artifactId;
	private FileSystem fs;
	private IndexTable indexTable;
	private void linkWithHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        //TODO
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
        
        conf.set("fs.defaultFS", "hdfs://Master:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.fs = FileSystem.get(conf);
	}
	private void loadIndexTable() throws IOException {
		this.indexTable = new IndexTable(fs, new Path("./Packages/" + artifactId));
	}
	HDFSPackage(String artifactId) throws IOException {
		this.artifactId = artifactId;
		linkWithHDFS();
    	if (fs.exists(new Path("./Collection/" + artifactId)) == false) {
            FSDataOutputStream os = fs.create(new Path("./Collection/" + artifactId));
            System.out.println("Created FileSet " + artifactId);
            os.close();
    	}
    	if (fs.exists(new Path("./Packages/" + artifactId)) == false) {
            FSDataOutputStream os = fs.create(new Path("./Packages/" + artifactId));
            os.close();
    	}
		loadIndexTable();
        System.out.println("Linked with FileSet " + artifactId);
	}
	public void close() throws IOException {
		indexTable.saveTable();
        fs.close();
	}
	protected void finalize() throws IOException {
		close();
	}
    //从hdfs的指定Package中取出指定文件，保存到本地
	public void getFileFromPackage(String fileName, String savePath) throws IOException {
		long[] info = indexTable.getRecord(fileName);
		if (info == null) {
			
		} else {
			long fileSize = info[0];
			long fileOffset = info[1];
			FSDataInputStream in = fs.open(new Path("./Collection/" + artifactId));
			byte[] bytes = new byte[(int)fileSize];
			//TODO it seems read() might only read l<INPUT_SIZE bytes and return l.
			//but anyway I didn't encounter with that situation.
	        int readed = in.read(fileOffset, bytes, 0, (int)fileSize);
	        if (readed != fileSize) {
	        	System.out.println("Error: readed from hadoop file is not equal to fileSize.");
	        	throw new IOException();
	        }
	        
			in.close();
			
			File file = new File(savePath);
			if (file.exists() == false)
				file.createNewFile();
			FileOutputStream out = new FileOutputStream(file);
			out.write(bytes, 0, (int)fileSize);
			out.close();
		}
    }
	public void getFilesFromPackage(String savePath) {
		List<String> fileNames = indexTable.getList();
		fileNames.stream().forEach(name -> {
			try {
				getFileFromPackage(name, savePath + "/" + name);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}
    //将本地的指定文件放入指定Package中
	private void putFileIntoPackage(File file) throws IllegalArgumentException, IOException {
        if (file.exists() == false || file.isDirectory() == true) {
        	return;
        }
		String fileName = file.getName();
        long fileSize = file.length();
        
        System.out.println("{PUT: {file_name: " + fileName + ", file_path_in_local: " + file.getPath() + "}}");
        
        FileInputStream in = new FileInputStream(file);
        byte[] bytes = new byte[(int) fileSize];
        int readed = in.read(bytes);
        if (readed != fileSize) {
        	System.out.println("Error: readed is not equal to fileSize.");
        	throw new IOException();
        }
        
        FSDataOutputStream os = fs.append(new Path("./Collection/"+artifactId));
        long fileOffset = os.getPos();
        
        os.write(bytes, 0, (int) fileSize);
        os.close();
        
        indexTable.addRecord(fileName, fileSize, fileOffset);
    }
    //将指定file directory/file下的所有文件放入该Package中
	public void putFilesIntoPackage(String filePath) throws IllegalArgumentException, IOException {
		 File file = new File(filePath);
		 if (file.isDirectory() == true) {
	    	 File[] files = file.listFiles();
	    	 for (File f : files) {
	    		 putFileIntoPackage(f);
	    	 }
	     } else {
    		 putFileIntoPackage(file);
	     }
    }
	public void list() {
    	System.out.println("FileSet {" + artifactId + "} has : \nFILE_NAME FILE_SIZE FILE_OFFSET");
		indexTable.showList();
	}
}