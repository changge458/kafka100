package com.qst.kafka100;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import org.junit.Test;

public class TestSendFile {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length != 3){
			System.out.println("need 3 arguments");
			System.exit(0);
		}
		if(args[0].equals("0")){
			sendFile(args[1],args[2]);
		}
		else{
			sendFile2(args[1],args[2]);
		}
		
		
		
	}
	public static void sendFile(String src, String dest) throws Exception{
		FileInputStream fis = new FileInputStream(src);
		FileOutputStream fos = new FileOutputStream(dest);
		int len = 0;
		byte[] buf = new byte[1024];
		long start = System.currentTimeMillis();
		while( (len = fis.read(buf)) != -1){
			fos.write(buf, 0, len);
		}
		fis.close();
		fos.close();
		System.out.println("ok");
		System.out.println(System.currentTimeMillis() - start);
	}
	
	public static void sendFile2(String src, String dest) throws Exception{
		File f = new File(src);
		FileInputStream fis = new FileInputStream(src);
		FileOutputStream fos = new FileOutputStream(dest);
		
		FileChannel inChannel = fis.getChannel();
		FileChannel outChannel = fos.getChannel();
		
		long start = System.currentTimeMillis();
		inChannel.transferTo(0, f.length(), outChannel);
		
		inChannel.close();
		outChannel.close();
		
		fis.close();
		fos.close();
		System.out.println("ok");
		System.out.println(System.currentTimeMillis() - start);
	}

}
