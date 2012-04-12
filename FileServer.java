import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;

public class FileServer {
    
    String myPath = "/fileserver";
    ZkConnector zkc;
    Watcher watcher;
    String filename = "/md5/dictionary/lowercase.rand"

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
 
        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };
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
            int linenumbers;
            linenumbers = count(filename);
            int partition = linenumbers/10;
            /*
             * TODO: add the partition code and Bob;s email code here
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
