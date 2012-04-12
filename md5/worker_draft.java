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

class CID_struct implements Serializable {
	public String  CID;
	static List<String> JID_Set; 
	
	/* constructor */
	public CID_struct(String CID) {
		JID_Set = new ArrayList<String>();
		this.CID = CID;
	}	
}

public class worker_draft {

	static String workersPath = "/workers";
	static String statusPath = "/status";
	static String workerID = "0";
	static List<String> CID_Set = new ArrayList<String>();
	static HashMap JID_Set = new HashMap();
	
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
        	List<String> children = listenClients(zk, statusPath);
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
        
        
        
        System.out.println("Just chillin'...");
        
        try{       
            nodeCreatedSignal.await();
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

        System.out.println("DONE");
    }
    
    // Listens to new clients (CID)
    private static List<String> listenClients (final ZooKeeper zk, final String path) throws Exception {
		List<String> clients = zk.getChildren(
			path,
			new Watcher() { 
				public void process(WatchedEvent event) {
				    // check for event type NodeChildrenChanged
				    boolean NodeChildrenChanged  = event.getType().equals(EventType.NodeChildrenChanged);
				    if (NodeChildrenChanged) {
				    	System.out.println("Children of " + path + " have changed.");
				    	try {
				    		List<String> new_children  = listenClients(zk, path);
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
		while(i < clients.size()) {
        	if(!CID_Set.contains(clients.get(i))) {
        		// Add the CID to the CID set
        		CID_Set.add(clients.get(i));
        		// Add the CID to the JID hashmap
        		JID_Set.put(clients.get(i), new ArrayList<String>());
        		// Set a listener on that node
        		listenJobs(zk, path + "/" + clients.get(i), clients.get(i));
        		System.out.println("Added CID: " + clients.get(i));
        	}
        	i++;
        }
		// Remove the newly removed nodes (if one client has been removed)
		i = 0;
		while(i < CID_Set.size()) {
        	if(!clients.contains(CID_Set.get(i))) {
        		System.out.println("Removing CID: " + CID_Set.get(i));
        		// Remove the CID from the CID set
        		CID_Set.remove(CID_Set.get(i));
        		// Remove the CID from the JID hashmap
        		JID_Set.remove(CID_Set.get(i));
        	}
        	i++;
        }
		return clients;
	}
	
	// Listens to new jobs (JID)
	private static void listenJobs (final ZooKeeper zk, final String path, final String CID) throws Exception {
		List<String> jobs = zk.getChildren(
			path,
			new Watcher() { 
				public void process(WatchedEvent event) {
				    // check for event type NodeChildrenChanged
				    boolean NodeChildrenChanged  = event.getType().equals(EventType.NodeChildrenChanged);
				    if (NodeChildrenChanged) {
				    	System.out.println("Children of " + path + " have changed.");
				    	try {
				    		listenJobs(zk, path, CID);
				    	} catch(KeeperException e) {
							System.out.println(e.code());
						} catch(Exception e) {
							System.out.println("Make node:" + e.getMessage());
						}
				    }
				}
			}
		);
		// Add the new nodes (if one job has been added)
		List<String> JID_list = (List<String>) JID_Set.get(CID);
		int i = 0;
		while(i < jobs.size()) {
        	if(!JID_list.contains(jobs.get(i))) {
        		// Add the CID to the CID set
        		JID_list.add(jobs.get(i));
        		// Set a watch on the JID node
        		listenStatus(zk, path + "/" + jobs.get(i));
        		System.out.println("Added JID: " + jobs.get(i) + " to CID " + CID);
        	}
        	i++;
        }
        // Remove the newly removed nodes (if one job has been removed)
		i = 0;
		while(i < ((List<String>)JID_Set.get(CID)).size()) {
        	if(!jobs.contains(((List<String>)JID_Set.get(CID)).get(i))) {
        		System.out.println("Removing JID: " + ((List<String>)JID_Set.get(CID)).get(i) + " from CID " + CID);
        		// Remove the JID from the JID hashmap
        		((List<String>)JID_Set.get(CID)).remove((((List<String>)JID_Set.get(CID)).get(i)));
        	}
        	i++;
        }
		
	}
	
	
	private static void listenStatus (final ZooKeeper zk, final String path) throws Exception {
		byte b[] = zk.getData(
            path, 
            new Watcher() {       // Anonymous Watcher
                @Override
                public void process(WatchedEvent event) {
                	// Check for data modifications from the job tracker
                    boolean NodeDataChanged = event.getType().equals(EventType.NodeDataChanged);
                    if (NodeDataChanged) {
                        try {
                        	byte data_bytes[] = zk.getData(path, false, null);
				    		String data = new String(data_bytes);
				    		System.out.println("Data from status node " + path + " is: " + data);
				    		// Set the watch again
				    		listenStatus(zk, path);
                        } catch(KeeperException e) {
							System.out.println(e.code());
						} catch(Exception e) {
							System.out.println("Make node:" + e.getMessage());
						}	
                    }
                }
            },
            null);
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
