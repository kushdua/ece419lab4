import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

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
    String filename = "/md5/dictionary/lowercase.rand";
    static CountDownLatch nodeDeletedSignal = new CountDownLatch(1);

    public static void main(String[] args) {
      
        if (args.length != 1) {
            System.out.println("Usage: java -classpath example1/lib/zookeeper-3.3.2.jar:example1/lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort");
            return;
        }

        FileServer t = new FileServer(args[0]);   
 
        System.out.println("Sleeping...");
        try {
            Thread.sleep(500);
        } catch (Exception e) {}
        
        t.checkpath();
        
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
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
            
            /* --------------- CODE FLOW ------------
             * 1.Calculate file partition estimates
             * 2.Read in the file partitions
             * 3.Store the file partitions back to the disk 
             * 4. Put the URL locations in znodes under /directory 
             */
            int linenumbers=0;
            try {
				linenumbers = count(filename);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            int partition = linenumbers/10;
            /*
             * TODO: add the partition code and Bob's email code here (ELECTION CODE'S BEEN ADDED ABOVE)
             */
            
            
            
            
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
    
    public int count(String filename) throws IOException {
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
