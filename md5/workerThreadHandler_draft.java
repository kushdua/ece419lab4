import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.io.*;
import java.util.*;

public class workerThreadHandler_draft extends Thread {

	// Internal class variables
	ZooKeeper zk;
	String JID_path;
	String WID;
	String dictionaryURL;
	String pwdHash;
	


	public workerThreadHandler_draft(ZooKeeper zk, String WID, String JID_path, String dictionaryURL, String pwdHash) {
		super("workerThreadHandler_draft");
		this.zk            = zk;
		this.WID           = WID;
		this.JID_path      = JID_path;
		this.dictionaryURL = dictionaryURL;
		this.pwdHash       = pwdHash;
		System.out.println("Created new thread to handle a client connections");
	}
	
	public void run() {
		//1 Execute the dictionary attack on the input hash
		String password = findHash(dictionaryURL, pwdHash);
		System.out.println("The password is: " + password);
	}
	
	private static String findHash(String dictionaryURL, String pwdHash) {
	
		String pwd = "";
		boolean found = false;
		try {
			FileInputStream fstream = new FileInputStream(dictionaryURL);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while (((line = br.readLine()) != null) && (found == false)) {
				if (getHash(line).equals(pwdHash)) {
					System.out.println("FOUND!!!");
					found = true;
					pwd = line;
				}
			}
			
			in.close();
		} catch (Exception e) {
			/* just print the error stack and exit. */
			e.printStackTrace();
			System.exit(1);
		}
		if (found == false) {
			pwd = "NOT FOUND";
		}
		
		return pwd;
	}
	
    public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }

}
