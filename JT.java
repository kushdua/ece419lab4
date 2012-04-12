import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class JT {
    
    static CountDownLatch nodeDeletedSignal = new CountDownLatch(1);
    static String JT_PATH="/jobtracker";
    private static int localPort=-1;
    
    public static String JOB_PENDING="0";
    public static String JOB_IN_PROGRESS="1";
    public static String JOB_COMPLETED_NOT_FOUND="2";
    public static String JOB_COMPLETED_FOUND="3";
    
	static List<String> WID_Set = new ArrayList<String>();
    
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
            	String IP=serverSocket.getInetAddress().getHostAddress();//getHostName();
        		
    			if(IP.equals("localhost") || IP.equals("127.0.0.1") || IP.equals("0.0.0.0"))
    			{
    				IP=InetAddress.getLocalHost().getHostAddress();
    			}
    			String location=IP+":"+localPort;
    			
                System.out.println("Creating " + JT_PATH);
                String path=zk.create(
                    JT_PATH,         // Path of znode
                    location.getBytes(),           // Data not needed.
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

        try {
        	List<String> children = listenClients(zk);
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
        
        
        try
        {
        	String path=zk.create(
	                "/status",         // Path of znode
	                null,           // Data not needed.
	                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
	                CreateMode.PERSISTENT   // Znode type, set to Ephemeral.
	                );
        } catch(KeeperException e) {
        	if(e.code()==KeeperException.Code.NODEEXISTS)
        	{
        		//Do nothing... client used our services before :-)
        	}
        } catch(Exception e) {
        	//Send error message below to the client...
        }
        
        //Start listening on serverPort
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
	
    // Listens to workers changes (WID)
    private static List<String> listenClients (final ZooKeeper zk) throws Exception {
		List<String> clients = zk.getChildren(
			"/workers",
			new Watcher() { 
				public void process(WatchedEvent event) {
				    // check for event type NodeChildrenChanged
				    boolean NodeChildrenChanged  = event.getType().equals(EventType.NodeChildrenChanged);
				    if (NodeChildrenChanged) {
				    	System.out.println("Children of /workers have changed.");
				    	try {
				    		List<String> new_children  = listenClients(zk);
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
        	if(!WID_Set.contains(clients.get(i))) {
        		// Add the CID to the CID set
        		WID_Set.add(clients.get(i));
        		System.out.println("Added WID: " + clients.get(i));
        	}
        	i++;
        }
		
		// Remove the newly removed nodes (if one client has been removed)
		i = 0;
		while(i < WID_Set.size()) {
        	if(!clients.contains(WID_Set.get(i))) {
        		String removedWorker=WID_Set.get(i);
        		System.out.println("Removing CID: " + removedWorker);
        		// Remove the CID from the CID set
        		WID_Set.remove(i);
        		
        		rebalanceJobsAfterWorkerCrash(zk, removedWorker);
        	}
        	i++;
        }
		return clients;
	}

	private static void rebalanceJobsAfterWorkerCrash(ZooKeeper zk, String removedWorker) {
		//GET STATUS
		List<String> clients=null;
		List<String> jobs=null;
		
		try {
			clients = zk.getChildren(
					"/status",
					null);
		} catch (KeeperException e) {
			jobs=null;
		} catch (InterruptedException e) {
			jobs=null;
		}
		
		for(String client : clients)
		{
			jobs=null;
			String output="";
			try {
				jobs = zk.getChildren(
						"/status",
						null);
			} catch (KeeperException e) {
				jobs=null;
			} catch (InterruptedException e) {
				jobs=null;
			}
			
			for(String job : jobs)
			{
				Stat nodeStat=new Stat();
				boolean validJobData=true;
				output+="";
				
				boolean updateSuccess=false;
				boolean modifiedJobSpec=false;
				int nextWI=0;

				while(updateSuccess==false)
				{
					modifiedJobSpec=false;
					//MODIFY JOB FILE
					try {
						byte[] jobValue=zk.getData("/status/"+client+"/"+job,
								false,
								nodeStat);
						String value=new String(jobValue);
						String[] parts=value.split(";");
						for(String part : parts)
						{
							String[] values=part.split(",");
							if(values.length==5)
							{
								if(	values[0].equals(removedWorker) &&
									(values[3].equals(JT.JOB_PENDING) ||
									 values[3].equals(JT.JOB_IN_PROGRESS))
								  )
								{
									nextWI=(int)(Math.random()*WID_Set.size());
									output+=WID_Set.get(
											(nextWI>=WID_Set.size())?0:nextWI
										);
									output+=","+values[1]+","+values[2]+","+
											values[3]+","+values[4]+";";
									modifiedJobSpec=true;
								}
								else
								{
									output+=part;
								}
							}
							else
							{
								validJobData=false;
							}
						}
					} catch (KeeperException e) {
						System.out.println("Could not retrieve job data.");
					} catch (InterruptedException e) {
						System.out.println("Could not unexpectedly retrieve job data.");
					}

					//Invalid job progress contents
					//if(validJobData==false)
					//{
					//	output+="UNKNOWN\t\t-;";
					//}
					
					//UPLOAD UPDATED JOB FILE BACK
					if(modifiedJobSpec==true)
					{
						try
						{
							zk.setData("/status/"+client+"/"+job,output.getBytes(),nodeStat.getVersion());
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
						} catch (InterruptedException e) {
							updateSuccess=false;
						}
					}
					else
					{
						//Proceed to next job... nothing to update in this one
						updateSuccess=true;
					}
				}
			}
		}
	}
}

class ClientHandler extends Thread
{
	private Socket socket = null;
	private String CID="";
	public ObjectOutputStream toPlayer=null;
	public ObjectInputStream fromplayer=null;
	ZooKeeper zk=null;
	
	public ClientHandler(Socket accept, ZooKeeper zk) {
		this.socket = accept;
		this.zk=zk;
	}

	public void run() {
		//Client connected => listen for messages
        try {
			BrokerPacket fromclientpacket = null;
			while((fromclientpacket = (BrokerPacket) fromplayer.readObject())!=null){
				if(fromclientpacket.type==BrokerPacket.BROKER_passid)
				{
					CID=fromclientpacket.symbol;
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
							BrokerPacket toclient=new BrokerPacket();
					        toclient.type=BrokerPacket.BROKER_passid;
							toclient.symbol="Successfully recorded your client ID.";
							toPlayer.writeObject(toclient);
							continue;
			            }
			        } catch(KeeperException e) {
			        	if(e.code()==KeeperException.Code.NODEEXISTS)
			        	{
			        		//Do nothing... client used our services before :-)
							BrokerPacket toclient=new BrokerPacket();
					        toclient.type=BrokerPacket.BROKER_passid;
							toclient.symbol="Successfully recorded your client ID.";
							toPlayer.writeObject(toclient);
			        		continue;
			        	}
			        } catch(Exception e) {
			        	//Send error message below to the client...
			        }
			        

					BrokerPacket toclient=new BrokerPacket();
			        toclient.type=BrokerPacket.BROKER_passid;
					toclient.symbol="Error: Could not record your client ID.";
					toPlayer.writeObject(toclient);
				}
				else if(fromclientpacket.type==BrokerPacket.BROKER_submitquery)
				{
					//SUBMIT JOB
					String inputHash=fromclientpacket.symbol;
			        String output="";
			        
			        //Get active workers
					List<String> workers=null;
					try {
						workers = zk.getChildren(
								"/workers",
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
								BrokerPacket toclient=new BrokerPacket();
						        toclient.type=BrokerPacket.BROKER_submitquery;
								toclient.symbol="Error: Could not submit new job due to problem with dictionary.";
								toPlayer.writeObject(toclient);
							} catch (InterruptedException e) {
								BrokerPacket toclient=new BrokerPacket();
						        toclient.type=BrokerPacket.BROKER_submitquery;
								toclient.symbol="Error: Could not submit new job due to unexpected problem with dictionary.";
								toPlayer.writeObject(toclient);
							}
							
							output+=workers.get(i)+","+new String(dictURI)+","+inputHash+","+JT.JOB_PENDING+",-;";
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
			        		BrokerPacket toclient=new BrokerPacket();
					        toclient.type=BrokerPacket.BROKER_submitquery;
							toclient.symbol="Error: Could not submit new job.";
							toPlayer.writeObject(toclient);
			        	}
			        } catch(Exception e) {
			        	BrokerPacket toclient=new BrokerPacket();
				        toclient.type=BrokerPacket.BROKER_submitquery;
						toclient.symbol="Error: Could not submit new job due to unexpected problem.";
						toPlayer.writeObject(toclient);
			        }
			        
			        //Send results to client
			        int JID=Integer.parseInt(path.substring(path.lastIndexOf('/')).trim());
			        BrokerPacket toclient=new BrokerPacket();
			        toclient.type=BrokerPacket.BROKER_submitquery;
					toclient.symbol="Successfully submitted job "+JID+".";
					toPlayer.writeObject(toclient);
				}
				else if(fromclientpacket.type==BrokerPacket.BROKER_jobqueue)
				{
					//GET STATUS
					List<String> jobs=null;
					String output="JOB ID\tSTATUS\t\tPASSWORDn";
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
						output+=elem+"\t";
						try {
							byte[] jobValue=zk.getData("/status/"+CID+"/"+elem,
									false,
									nodeStat);
							String value=new String(jobValue);
							int total=0, completed=0;
							String[] parts=value.split(";");
							for(String part : parts)
							{
								String[] values=part.split(",");
								if(values.length==5)
								{
									total++;
									if(	values[3].equals(JT.JOB_COMPLETED_FOUND) ||
										values[3].equals(JT.JOB_COMPLETED_NOT_FOUND))
									{
										completed++;
									}
									
									if(values[3].equals(JT.JOB_COMPLETED_FOUND))
									{
										output+="COMPLETE\t\t"+values[4]+";";
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
								output+="COMPLETE\t\t-;";
							}
							else
							{
								output+=((completed*100)/total)+"%\t\t-;";
							}
						} catch (KeeperException e) {
							output+="UNKNOWN\t\t-;";
						} catch (InterruptedException e) {
							output+="UNKNOWN\t\t-;";
						}

						//Invalid job progress contents
						if(validJobData==false)
						{
							output+="UNKNOWN\t\t-;";
						}
						
						//Send results to client
						BrokerPacket toclient=new BrokerPacket();
				        toclient.type=BrokerPacket.BROKER_jobqueue;
						toclient.symbol=output;
						toPlayer.writeObject(toclient);
					}
				}
				else if(fromclientpacket.type==BrokerPacket.BROKER_BYE)
				{
					fromplayer.close();
					toPlayer.close();
					socket.close();
					break;
				}
			}
		} catch (SocketException e) {
			//Remove socket and any game objects (client from maze, projectiles, etc)
			System.err.println("SocketException generated. Game client most likely disconnected.");
		} catch (EOFException e) {
			//Remove socket and any game objects (client from maze, projectiles, etc)
			System.err.println("EOFException generated. Game client most likely disconnected.");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

    public Socket getClientSocket() {
        return socket;
    }
}
