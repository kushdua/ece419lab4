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
import java.util.concurrent.CountDownLatch;

public class worker_draft {

	static String workersPath = "/workers";
	static String statusPath = "/status";
	static String workerID = "0";
	static List<String> CID_Set = new ArrayList<String>();
	
	static CountDownLatch nodeCreatedSignal = new CountDownLatch(1);

    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. worker_draft zkServer:clientPort WID");
            System.out.println(args.length);
            return;
        }
        
        workerID = args[1];

        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper();
        
        // 1: Check if the /workers node exists. If it doesn't create the new persistent node.
        try {
        	System.out.println("Checking if /worker exists");
        	if(zk.exists(workersPath, false)==null) {
        		System.out.println("Znode /worker does not exist");
        		zk.create(
		            workersPath,    // Path of znode
		            null,           // Data not needed.
		            Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
		            CreateMode.PERSISTENT   // Znode type, set to ephemeral.
		            );
		        System.out.println("Znode /worker added successfully");
        	} else {
        		System.out.println("Znode /worker already exists");
        	}
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
		
		//2: Add the WID child (ephemeral) to the /workers znode
        try {
            System.out.println("Creating " + workersPath + "/" + workerID);
            zk.create(
                workersPath + "/" + workerID,  // Path of znode
                null,                          // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.EPHEMERAL    // Znode type, set to ephemeral.
                );

        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
        
        //3: Get the children of the /status node (collect all of the CIDs) and set a watch on the node
        try {
        	List<String> children = grabChildren(zk, statusPath);
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
        
        
        
        System.out.println("Waiting for " + statusPath + " children to be created ...");
        
        try{       
            nodeCreatedSignal.await();
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

        System.out.println("DONE");
    }
    
    private static List<String> grabChildren (final ZooKeeper zk, final String path) throws Exception {
		List<String> children = zk.getChildren(
			path,
			new Watcher() { 
				public void process(WatchedEvent event) {
				    // check for event type NodeChildrenChanged
				    boolean NodeChildrenChanged  = event.getType().equals(EventType.NodeChildrenChanged);
				    if (NodeChildrenChanged) {
				    	System.out.println("Children of " + path + " have changed.");
				    	try {
				    		List<String> new_children  = grabChildren(zk, path);
				    	} catch(KeeperException e) {
							System.out.println(e.code());
						} catch(Exception e) {
							System.out.println("Make node:" + e.getMessage());
						}
				    }
				}
			}
		);
		// Add the new nodes (if one client has been added)
		int i = 0;
		while(i < children.size()) {
        	if(!CID_Set.contains(children.get(i))) {
        		CID_Set.add(children.get(i));
        		System.out.println("Added:" + children.get(i));
        	}
        	i++;
        }
		// Remove the newly removed nodes (if one client has been removed)
		i = 0;
		while(i < CID_Set.size()) {
        	if(!children.contains(CID_Set.get(i))) {
        		System.out.println("Removing:" + CID_Set.get(i));
        		CID_Set.remove(CID_Set.get(i));
        	}
        	i++;
        }
		return children;
	}
    
    private static String findHash(String hash) {
	
		String URL = "dictionary/lowercase.rand";
		String pwd = "";
		boolean found = false;
		try {
			FileInputStream fstream = new FileInputStream(URL);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while (((line = br.readLine()) != null) && (found == false)) {
				if (getHash(line).equals(hash)) {
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
