import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestCopyFile {

	static String hdfsUri = "hdfs://172.16.22.146:8000/";

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(URI.create(hdfsUri), conf);
		File dir = new File("D:/UploadHDFSFile/test/");
		File[] fileList = dir.listFiles();
		Path[] paths = null;
		if (fileList != null) {
			paths = new Path[fileList.length];
			for (int i = 0; i < fileList.length; i++) {
				paths[i] = new Path(fileList[i].getAbsolutePath());
			}
		}
		if (paths != null && paths.length > 0) {
			String hdfsUploadUri = hdfsUri + "user/lonka.liu/input02";
			Path dst = new Path(hdfsUploadUri);
			if (CheckAndCreateDir(hdfs, dst)) {
				hdfs.copyFromLocalFile(false, true, paths, dst);
				System.out.println("Upload to" + conf.get("fs.default.name"));
				FileStatus files[] = hdfs.listStatus(dst);
				for (FileStatus file : files) {
					System.out.println(file.getPath());
				}
			}
		}
	}

	public static boolean CheckAndCreateDir(FileSystem hdfs, Path dirPath)
			throws IOException {
		if (!hdfs.exists(dirPath)) {
			hdfs.mkdirs(dirPath);
		}
		return true;
	}
}
