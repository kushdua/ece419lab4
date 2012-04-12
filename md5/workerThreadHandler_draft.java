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

public class workerThreadHandler_draft extends Thread {

	// Internal class variables
	ZooKeeper zk=null;
	String JID_path="";
	String WID="";
	String dictionaryURL="";
	String pwdHash="";
	private boolean foundAnswer=false;
	private String theAnswerIFound="";

	public workerThreadHandler_draft(ZooKeeper zk, String WID, String JID_path, String dictionaryURL, String pwdHash) {
		super("workerThreadHandler_draft");
		this.zk            = zk;
		this.WID           = WID;
		this.JID_path      = JID_path;
		this.dictionaryURL = dictionaryURL;
		this.pwdHash       = pwdHash;
		System.out.println("Created new thread to handle a client connections");
		
		//UPDATE THAT YOU'VE STARTED
		updateProgressInfo();
	}
	
	public void run() {
		//1 Execute the dictionary attack on the input hash
		String password = findHash(dictionaryURL, pwdHash);
		System.out.println("The password is: " + password);
		
		if(password.equals("NOT FOUND"))
		{
			foundAnswer=false;
		}
		else
		{
			foundAnswer=true;
			theAnswerIFound=password;
		}
		
		//UPDATE THAT YOU'RE DONE
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
	
	public void updateProgressInfo()
	{
		boolean updateSuccess=false;

		while(updateSuccess==false)
		{
			//CODE TO UPDATE JID PROGRESS ONCE A WORKER THREAD STARTS OR FINISHES
			try {
				Stat res=new Stat();
				byte data_bytes[] = zk.getData(JID_path, false, res);
				String dataIn = new String(data_bytes);
				String output="";
				//String[] lines=dataIn.split(";");
				String data=dataIn;
				//for(String data : lines){
				// Tokenize the String
					String[] tokens = data.split(",");
					if(!(tokens.length==5)) {
						System.out.println("Incorrect formatting: do nothing");
					} else {
						// Tokenize the string
						String WID           = tokens[0];
						String dictionaryURL = tokens[1];
						String pwdHash       = tokens[2];
						String status        = tokens[3];
						String answer        = tokens[4];
						// Check if the worker matches our WID
						if (WID.equals(this.WID) && dictionaryURL.equals(this.dictionaryURL)) {
							// Check if the worker has already processed/is processing this request 
							if(status.equals("1")) {
								if(foundAnswer==true)
								{
									output+=WID+","+dictionaryURL+","+pwdHash+","+"3"+theAnswerIFound+";";
								}
								else
								{
									output+=WID+","+dictionaryURL+","+pwdHash+","+"2"+"-;";
								}
							}
							else if(status.equals("0")) {
								//Indicate we spawned this worker thread to work on this job...
								output+=WID+","+dictionaryURL+","+pwdHash+","+"1"+"-;";
							}
							else
							{
								//WTF.. Someone changed this for our thread.. Multiple threads with same ID?
							}
						}
						else
						{
							output+=data;
						}
					}
				//}
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
					}
					else if(ke.code().equals(Code.CONNECTIONLOSS))
					{
						updateSuccess=false;
					}
				}
			} catch(KeeperException e) {
				System.out.println(e.code());
			} catch(Exception e) {
				System.out.println("Make node:" + e.getMessage());
			}
		}
	}
}
