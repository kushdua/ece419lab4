import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.*;
import java.util.*;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

public class FileServer {
    
    String myPath = "/fileserver";
    ZkConnector zkc;
    Watcher watcher;
    static CountDownLatch nodeDeletedSignal = new CountDownLatch(1);

    public static void main(String[] args) {
      
        if (args.length != 1) {
            System.out.println("Usage: java -classpath example1/lib/zookeeper-3.3.2.jar:example1/lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort");
            return;
        }

        FileServer t = new FileServer(args[0]);   
 
        System.out.println("Sleeping...");
        try {
            Thread.sleep(50);
        } catch (Exception e) {}
        
        t.checkpath();
        
        System.out.println("Sleeping...");
        while (true) {
           // try{ Thread.sleep(5000); } catch (Exception e) {}
        }
        
        
        
        
        
    }

    public FileServer(String hosts) {
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
 
        Stat ret=null;
        int time=0;
        
        ZooKeeper zk=zkc.getZooKeeper();
        
        while(ret==null)
        {
        	//See if leader node for JT exists
	        try {
	            ret=zk.exists(
	                myPath, 
	                (time==0)?(new Watcher() {       // Anonymous Watcher
	                    @Override
	                    public void process(WatchedEvent event) {
	                        // check for event type NodeCreated
	                        boolean isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
	                        // verify if this is the defined znode
	                        boolean isMyPath = event.getPath().equals(myPath);
	                        if (isNodeDeleted && isMyPath) {
	                            nodeDeletedSignal.countDown();
	                        }
	                    }
	                }):null);
	        } catch(KeeperException e) {
	            System.out.println(e.code());
	        } catch(Exception e) {
	            System.out.println(e.getMessage());
	        }
	        
	        //Wait for deletion or create node if ret==null
	        if(ret!=null)
	        {
		        try{
		            nodeDeletedSignal.await();
		            time=-1;
		        } catch(Exception e) {
		            System.out.println(e.getMessage());
		        }
	        }
	        
        	//Create znode
            try {
            	String IP=InetAddress.getLocalHost().getHostAddress();//serverSocket.getInetAddress().getHostAddress();//getHostName();
        		
    			//if(IP.equals("localhost") || IP.equals("127.0.0.1"))
    			//{
    			//	IP=InetAddress.getLocalHost().getHostAddress();
    			//}
    			String location=IP;//+localPort;
    			
                System.out.println("Creating " + myPath);
                String path=zk.create(
                    myPath,         // Path of znode
                    location.getBytes(),           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.EPHEMERAL   // Znode type, set to Ephemeral.
                    );
	            //TODO: Remove println
	            System.out.println("Created FS leader znode "+path);
                if(path.equals(myPath))
                {
                	break;
                }
            } catch(KeeperException e) {
            	if(e.code()==KeeperException.Code.NODEEXISTS)
            	{
            		ret=null;
            		//Don't add another watch listener
            		time++;
            	}
            } catch(Exception e) {
                System.out.println("Make node:" + e.getMessage());
            }
        }

        /* --------------- CODE FLOW ------------
         * 1.Calculate file partition estimates
         * 2.Read in the file partitions
         * 3.Store the file partitions back to the disk 
         * 4. Put the URL locations in znodes under /directory 
         */
        String filename = "md5/dictionary/lowercase.rand";
        int linenumbers=0;
        int number_of_partitions = 10;
        try {
			linenumbers = count(filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int partitionsize = linenumbers/number_of_partitions;
  	  	System.out.println ("Total lines:"+linenumbers);
  	  	System.out.println ("Partition size is :"+partitionsize);
        
    	//child node creation code    
	    String path="";
	    try {
            path=zk.create(
                "/dictionary",         // Path of znode
                null,           // Dictionary data to store
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.PERSISTENT   // Znode type, set to Ephemeral.
                );
           
            if(path.equals("/dictionary"))
            {
                //Success
            }
        } catch(KeeperException e) {
            if(e.code()==KeeperException.Code.NODEEXISTS)
            {
                //That's fine...
            }
        } catch(Exception e) {
            System.out.println("Make node error:" + e.getMessage());
            //Shouldn't be fine...
        }
        
        /*
         * Now read in the file and store the partitions to the disk
         */
        try{
        	  // Open the file that is the first 
        	  // command line parameter
        	  FileInputStream fstream = new FileInputStream(filename);
        	  // Get the object of DataInputStream
        	  DataInputStream in = new DataInputStream(fstream);
        	  BufferedReader br = new BufferedReader(new InputStreamReader(in));
        	  String strLine;
        	  String save_location = "/tmp/";
        	  int start_line = 0;
        	  int partitionID = 0;
        	  int part = 1;
        	  //Read File Line By Line
			  BufferedWriter out = null;
        	  System.out.println ("Loop begin");
        	  while ((strLine = br.readLine()) != null)   {
        			  try {
        				  if(start_line==partitionsize*partitionID && part<=number_of_partitions) {
        					  out = new BufferedWriter(new FileWriter(save_location+"dict_"+partitionID+".dat"));
        					  partitionID++;
        					  out.write(strLine);
    						  out.write("\n");
    			        	  start_line++;
        					  while(((strLine = br.readLine())!=null) && start_line<partitionsize*partitionID) {
        						  out.write(strLine);
        						  out.write("\n");
        			        	  start_line++;
        					  }
            				  out.close();
            				  part++;
								path="";
								String output=save_location+"dict_"+partitionID+".dat";
								try {
								     path=zk.create(
								         "/dictionary/"+partitionID,         // Path of znode
								         output.getBytes(),           // Dictionary data to store
								         Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
								         CreateMode.EPHEMERAL_SEQUENTIAL   // Znode type, set to Ephemeral.
								         );
								 } catch(KeeperException e) {
								     if(e.code()==KeeperException.Code.NODEEXISTS)
								     {
								         //Cannot happen with sequential... especially if ephemeral...
								     }
								 } catch(Exception e) {
								     System.out.println("Make node error:" + e.getMessage());
								     //WTF
								 }
        				  	}
        				  }
        				  catch (IOException e)
        				  {
        				  System.out.println("Exception");
        				  }
        	  //start_line++;
        	  // Print the content on the console
        	 // System.out.println (strLine);
        	  }
        	  System.out.println ("END OF LOOP");

        	  //Close the input stream
        	  in.close();
        	    }catch (Exception e){//Catch exception if any
        	  System.err.println("Error: " + e.getMessage());
        	  }

        //watcher = new Watcher() { // Anonymous Watcher
        //                    @Override
        //                    public void process(WatchedEvent event) {
        //                        handleEvent(event);
        //                
        //                    } };
    }
    
    private void checkpath() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("the boss!");
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                try{ Thread.sleep(0); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }
    
    public static int count(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            while ((readChars = is.read(c)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n')
                        ++count;
                }
            }
            return count;
        } finally {
            is.close();
        }
    }

}
