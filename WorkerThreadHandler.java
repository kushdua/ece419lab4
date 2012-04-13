import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.io.*;
import java.util.*;

public class WorkerThreadHandler extends Thread {

	// Internal class variables
	ZooKeeper zk=null;
	String JID_path="";
	String WID="";
	String dictionaryURL="";
	String pwdHash="";
	private boolean foundAnswer=false;
	private String theAnswerIFound="";

	public WorkerThreadHandler(ZooKeeper zk, String WID, String JID_path, String dictionaryURL, String pwdHash) {
		super("WorkerThreadHandler");
		this.zk            = zk;
		this.WID           = WID;
		this.JID_path      = JID_path;
		this.dictionaryURL = dictionaryURL;
		this.pwdHash       = pwdHash;
		System.out.println("[WorkerThread] Created new thread to handle a client connections");
		
		//UPDATE THAT YOU'VE STARTED
		updateProgressInfo();
	}
	
	public void run() {
		//0 Time the process
		long startTime = System.currentTimeMillis();

		
		//1 Execute the dictionary attack on the input hash
		String password = findHash(dictionaryURL, pwdHash);
		
		//2 Compute how long this took
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		
		if(password.equals("NOT FOUND"))
		{
			foundAnswer=false;	
			System.out.println("[WorkerThread] Did not find the password -- took " + elapsedTime + "ms");
		}
		else
		{
			foundAnswer=true;
			System.out.println("[WorkerThread] Found the password: " + password + " -- took " + elapsedTime + "ms");
			theAnswerIFound=password;
		}
		
		
		//3 Update the JID node with the result
		updateProgressInfo();
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
	
	public void updateProgressInfo()
	{
		boolean updateSuccess=false;

		while(updateSuccess==false)
		{
			//CODE TO UPDATE JID PROGRESS ONCE A WORKER THREAD STARTS OR FINISHES
			try {
				// This string holds the new value that we will write to the JID node
				String output="";
				// Store the stat
				Stat res=new Stat();
				// Get the data at the JID node from zk
				byte data_bytes[] = zk.getData(JID_path, false, res);
				String data = new String(data_bytes);
				
				//System.out.println("DIRT: "+data);
				
				// Tokenize the String (per WID)
				String[] WID_tokens = data.split(";");
				for (int i=0; i<WID_tokens.length; i++) {
					// Tokenize the String
					String[] tokens = WID_tokens[i].split(",");
					if(!(tokens.length==5)) {
						System.out.println("[WorkerThread] Incorrect formatting: do nothing");
					} else {
						// Tokenize the string
						String WID           = tokens[0];
						String dictionaryURL = tokens[1];
						String pwdHash       = tokens[2];
						String status        = tokens[3];
						String answer        = tokens[4];
						// Check if the worker matches our WID
						if (WID.equals(this.WID) && dictionaryURL.equals(this.dictionaryURL) && pwdHash.equals(this.pwdHash)) {
							// Check if the worker has already processed/is processing this request 
							if(status.equals("1")) {
								if(foundAnswer==true)
								{
									output+=WID+","+dictionaryURL+","+pwdHash+","+"3,"+theAnswerIFound+";";
								}
								else
								{
									output+=WID+","+dictionaryURL+","+pwdHash+","+"2,"+"-;";
								}
							}
							else if(status.equals("0")) {
								//Indicate we spawned this worker thread to work on this job...
								output+=WID+","+dictionaryURL+","+pwdHash+","+"1,"+"-;";
							}
							else
							{
								//Someone else was originally assigned but now we are.. we think.
								output+=WID_tokens[i]+";";
							}
						}
						else
						{
							output+=WID_tokens[i]+";";
						}
					}
				}
				try
				{
					zk.setData(JID_path,output.getBytes(),res.getVersion());
					updateSuccess=true;
				}
				catch(KeeperException ke)
				{
					if(ke.code().equals(Code.BADVERSION))
					{
						updateSuccess=false;
						System.out.println("[WorkerThread] BADVERSION! Try again." );
					}
					else if(ke.code().equals(Code.CONNECTIONLOSS))
					{
						updateSuccess=false;
						System.out.println("[WorkerThread] CONNECTIONLOSS! Try again.");
					}
				}
			} catch(KeeperException e) {
				System.out.println(e.code());
			} catch(Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
}
