import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class JT {
    
    static CountDownLatch nodeDeletedSignal = new CountDownLatch(1);
    static String JT_PATH="/jobtracker";
    private static int localPort=-1;
    
    public static String ANSWER_NOT_FOUND="No match was found";
    
    public static void main(String[] args) {
  
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JT zkServer:clientPort <local JT server listening port>");
            return;
        }
        else
        {
        	localPort=Integer.parseInt(args[1]);
        }
        
        ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(localPort);
		} catch (IOException e2) {
			System.err.println("Could not open server socket for incoming clients. Exiting...");
			System.exit(-1);
		}
    
        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper();
        Stat ret=null;
        int time=0;
        
        while(ret==null)
        {
        	//See if leader node for JT exists
	        try {
	            ret=zk.exists(
	                JT_PATH, 
	                (time==0)?(new Watcher() {       // Anonymous Watcher
	                    @Override
	                    public void process(WatchedEvent event) {
	                        // check for event type NodeCreated
	                        boolean isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
	                        // verify if this is the defined znode
	                        boolean isMyPath = event.getPath().equals(JT_PATH);
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
                System.out.println("Creating " + JT_PATH);
                String path=zk.create(
                    JT_PATH,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.EPHEMERAL   // Znode type, set to Ephemeral.
                    );
	            //TODO: Remove println
	            System.out.println("Created JT leader znode "+path);
                if(path.equals(JT_PATH))
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

        //Start serverPort to listen on
		try {
			//Keep listening for connecting clients
			while(true)
			{
				ClientHandler temp = new ClientHandler(serverSocket.accept(),zk);
				temp.fromplayer=new ObjectInputStream(temp.getClientSocket().getInputStream());
				temp.toPlayer=new ObjectOutputStream(temp.getClientSocket().getOutputStream());	
				temp.start();
			}
		} catch (IOException e) {
			if(serverSocket!=null)
			{
				System.err.println("Closing server socket at JobTracker side");
				try {
					serverSocket.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
		}
    }
}

class ClientHandler extends Thread
{
	private Socket socket = null;
	private int CID=-1;
	public ObjectOutputStream toPlayer=null;
	public ObjectInputStream fromplayer=null;
	ZooKeeper zk=null;
	
	public ClientHandler(Socket accept, ZooKeeper zk) {
		this.socket = accept;
		this.zk=zk;
	}

	public void run() {
		//Client connected => listen for messages
		
		if(1==1)
		{
			//TODO: Get CID from packet
			CID=1;
	    	//Create znode
	        try {
	            System.out.println("Creating " + CID);
	            String path=zk.create(
	                "/status/"+CID,         // Path of znode
	                null,           // Data not needed.
	                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
	                CreateMode.PERSISTENT   // Znode type, set to Ephemeral.
	                );
	            //TODO: Remove println
	            System.out.println("Created CID znode "+path);
	            if(path.equals("/status/"+CID))
	            {
	            	//Living success at success!
	            }
	        } catch(KeeperException e) {
	        	if(e.code()==KeeperException.Code.NODEEXISTS)
	        	{
	        		//Do nothing... client used our services before :-)
	        	}
	        } catch(Exception e) {
	            System.out.println("Make node error:" + e.getMessage());
	        }
		}
		else if(2==2)
		{
			//SUBMIT JOB
			//TODO: Get input hash...
			String inputHash="abc";
	        String output="";
	        
	        //Get active workers
			List<String> workers=null;
			try {
				workers = zk.getChildren(
						"/workers/",
						null);
			} catch (KeeperException e) {
				workers=null;
			} catch (InterruptedException e) {
				workers=null;
			}
			
			List<String> dictParts=null;
			try {
				dictParts = zk.getChildren(
						"/status/"+CID,
						null);
			} catch (KeeperException e) {
				dictParts=null;
			} catch (InterruptedException e) {
				dictParts=null;
			}

		
			//Compute JID contents
			Stat nodeStat=null;
			int currDict=0;
			while(currDict<dictParts.size())
			{
				for(int i=0; i<workers.size(); i++, currDict++)
				{
					byte[] dictURI=null;
					try {
						dictURI = zk.getData("/dictionary/"+dictParts.get(currDict),
								false,
								nodeStat);
					} catch (KeeperException e) {
						//TODO: Return client error - could not create the job... Rollback too
					} catch (InterruptedException e) {
						//TODO: Return client error - could not create the job... Rollback too
					}
					
					output+=workers.get(i)+","+new String(dictURI)+","+inputHash+",0,"+JT.ANSWER_NOT_FOUND+"\n";
				}
			}

			String path="";
			//Create JID sequential node under /submit/CID/
	        try {
	            path=zk.create(
	                "/status/"+CID+"/",         // Path of znode
	                output.getBytes(),           // Data to store
	                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
	                CreateMode.PERSISTENT   // Znode type, set to Ephemeral.
	                );
	            //TODO: Remove println
	            System.out.println("Created CID znode "+path);
	            if(path.equals("/status/"+CID))
	            {
	            	//Living success at success!
	            }
	        } catch(KeeperException e) {
	        	if(e.code()==KeeperException.Code.NODEEXISTS)
	        	{
	        		//TODO: Return error to user that we couldn't submit job...
	        	}
	        } catch(Exception e) {
	            System.out.println("Make node error:" + e.getMessage());
	        }
	        
	        int JID=Integer.parseInt(path.substring(path.lastIndexOf('/')).trim());
		}
		else if(3==3)
		{
			//GET STATUS
			List<String> jobs=null;
			String output="JOB ID\tSTATUS\\tPASSWORDn";
			try {
				jobs = zk.getChildren(
						"/status/"+CID,
						null);
			} catch (KeeperException e) {
				jobs=null;
			} catch (InterruptedException e) {
				jobs=null;
			}
			
			for(String elem : jobs)
			{
				Stat nodeStat=null;
				boolean validJobData=true;
				output+="elem\t";
				try {
					byte[] jobValue=zk.getData("/status/"+CID+"/"+elem,
							false,
							nodeStat);
					String value=new String(jobValue);
					int total=0, completed=0;
					String[] parts=value.split("\n");
					for(String part : parts)
					{
						String[] values=part.split(",");
						if(values.length==5)
						{
							total++;
							if(values[3].equals("1"))
							{
								completed++;
							}
							
							if(!values[4].equals(JT.ANSWER_NOT_FOUND))
							{
								output+="COMPLETE\t\t"+values[4]+"\n";
								break;
							}
						}
						else
						{
							validJobData=false;
						}
					}
					//Add progress + output here...
					if(total==completed)
					{
						output+="COMPLETE\t\t-\n";
					}
					else
					{
						output+="IN PROGRESS\t\t-\n";
					}
				} catch (KeeperException e) {
					output+="UNKNOWN\t\t-\n";
				} catch (InterruptedException e) {
					output+="UNKNOWN\t\t-\n";
				}

				//Invalid job progress contents
				if(validJobData==false)
				{
					output+="UNKNOWN\t-\n";
				}
			}
		}
/*        try {
			MazewarPacket fromclientpacket = null;
			while((fromclientpacket = (MazewarPacket) fromplayer.readObject())!=null){
				//TODO: DO YAH THING BRAH
			}	
		} catch (SocketException e) {
			//Remove socket and any game objects (client from maze, projectiles, etc)
			System.err.println("SocketException generated. Game client most likely disconnected.");
			//TODO: CLEAN UP YOUR SHIT
		} catch (EOFException e) {
			//Remove socket and any game objects (client from maze, projectiles, etc)
			System.err.println("EOFException generated. Game client most likely disconnected.");
			//TODO: CLEAN UP YOUR SHIT
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
*/	}

    public Socket getClientSocket() {
        return socket;
    }
}
