import java.io.*;
import java.net.*;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;


public class Client {
	
	 static CountDownLatch nodeCreatedSignal = new CountDownLatch(1);
	 static String myPath = "/jobtracker";
	    
	public static void main(String[] args){
		
		/*  ---------------CODE FLOW -----------------------------
		 * 1. Connect/Lookup with the zookeeper
		 * 2. retrieve the information and keep looking unless primary JT has been created
		 * 3. Procced forward and connect to JT and feel free to do the queries
		 */
		
		/*
		 * needs to lookup for the primary jobtracker information on the first place
		 */
		//System.out.println("The argument length is: "+args.length);
		if (args.length != 2) {
            System.out.println("Usage: java -classpath example1/lib/zookeeper-3.3.2.jar:example1/lib/log4j-1.2.15.jar:. Client zkServer:clientPort <client ID>");
            return;
        }
    
        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper();
        Stat exists=null;
        while(true)
        {
	        try {
	        	//while(exists==null)
	        	//{
		            exists=zk.exists(
		                myPath, 
		                new Watcher() {       // Anonymous Watcher
		                    @Override
		                    public void process(WatchedEvent event) {
		                        // check for event type NodeCreated
		                        boolean isNodeCreated = event.getType().equals(EventType.NodeCreated);
		                        // verify if this is the defined znode
		                        boolean isMyPath = event.getPath().equals(myPath);
		                        if (isNodeCreated && isMyPath) {
		                            //System.out.println(myPath + " created!");
		                            nodeCreatedSignal.countDown();
		                        }
		                    }
		                });
	        	//}
	
	            
	        } catch(KeeperException e) {
	            System.out.println(e.code());
	        } catch(Exception e) {
	            System.out.println(e.getMessage());
	        }
	                            
	        //System.out.println("Waiting for " + myPath + " to be created ...");
			//System.out.println("Please wait while we connect to Job Tracker");
	        
	        
	
	        if(exists==null)						//	//in the begening pass the id to the Jobtracker
				//	pts.type = BrokerPacket.BROKER_passid;
				//	pts.symbol = userInput.trim();
				//	/* send the new server packet */
				//	out.writeObject(pts);
	        {
	        	System.out.println("Waiting for JT to come online.");
		        try{
		            nodeCreatedSignal.await();
		        } catch(Exception e) {
		            //System.out.println(e.getMessage());
		        }
	        }
	        
	        //retrieve the information regarding the node
	        byte[] b = null;
	        try {
	            b = zk.getData(myPath, false, null);
	        } catch (KeeperException e) {
	            // We don't need to worry about recovering now. The watch
	            // callbacks will kick off any exception handling
	            //e.printStackTrace();
	        	nodeCreatedSignal=new CountDownLatch(1);
	        	exists=null;
	        } catch (InterruptedException e) {
	            return;
	        }
	
	        String thedata=new String(b);
	        //System.out.println(thedata);
	        //System.out.println("Connecting to the Jobtracker Please wait");
	        
	        /*
	         * AT this time primary Job tracker has been created and the TCP/IP 
	         * information including port number etc has been retrieved
	         */
	        
	        Socket brokerSocket = null;
			ObjectOutputStream out = null;
			ObjectInputStream in = null;
	
			//Open connection to broker, or exit if unsuccessful
			try {
				/* variables for hostname/port */
				String hostname = "localhost";
				int port = 4444;
				//Open connection only if appropriate command line arguments are provided
				//exit otherwise...
				if(args.length == 2 ) {
					String[] datasplit = thedata.split(":");
					hostname = datasplit[0];
					port = Integer.parseInt(datasplit[1]);
					
				} else {
					System.err.println("ERROR: Invalid arguments!");
					System.exit(-1);
				}
				brokerSocket = new Socket(hostname, port);
	
				out = new ObjectOutputStream(brokerSocket.getOutputStream());
				in = new ObjectInputStream(brokerSocket.getInputStream());
	
			} catch (UnknownHostException e) {
				//System.err.println("ERROR: Don't know where to connect!!");
				//Try to get new address from JT and reconnect
	        	nodeCreatedSignal=new CountDownLatch(1);
	        	exists=null;
				continue;
				//System.exit(1);
			} catch (IOException e) {
				//System.err.println("ERROR: Couldn't get I/O for the connection.");
				//Try to get new address from JT and reconnect
	        	nodeCreatedSignal=new CountDownLatch(1);
	        	exists=null;
				continue;
				//System.exit(1);
			}
	
			System.out.println("Connection to the Jobtracker Successfull");
			//Display prompt and set up required variables for getting user input
			System.out.println("Enter queries or x for exit:");
			System.out.print("> ");
			BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
			String userInput;
	
			while(true)
			{
				//Keep getting user input and transmitting to broker until client quits ('x')
				try {
					
					//in the begening pass the id to the Jobtracker
					BrokerPacket pts=new BrokerPacket();
					pts.type = BrokerPacket.BROKER_passid;
					pts.symbol = args[1];
					/* send the new server packet */
					out.writeObject(pts);
					
					while ((userInput = stdIn.readLine()) != null
							&& !userInput.trim().equals("x")) {
	
						/*
						 * This code is for sending packets to the JOB TRACKER
						 */
						pts = new BrokerPacket();
						//if(userInput.contains("passid"))
						//{
						//	//in the begening pass the id to the Jobtracker
						//	pts.type = BrokerPacket.BROKER_passid;
						//	pts.symbol = userInput.trim();
						//	/* send the new server packet */
						//	out.writeObject(pts);
						//}
						//else if(userInput.contains("submitquery")) {
						if(userInput.contains("submitquery")) {
							//Send query request to JT
							pts.type = BrokerPacket.BROKER_submitquery;
							pts.symbol = userInput.substring(userInput.lastIndexOf("submitquery")+11).trim();
							/* send the new server packet */
							out.writeObject(pts);
						} 
						else if(userInput.contains("jobqueue")) {
							//Send queue request to JT
								pts.type = BrokerPacket.BROKER_jobqueue;
								pts.symbol = userInput.substring(userInput.lastIndexOf("jobqueue")+8).trim();
								/* send the new server packet */
								out.writeObject(pts);
						} else {
							System.out.println("Invalid Argument, Please Try Again");
							System.out.print("> ");
							continue;
						}
	
						
						/*
						 * This code is for receiving packets from JobTracker
						 */
	
						/* print server reply */
						BrokerPacket pfs = null;
						pfs = (BrokerPacket) in.readObject();
	
						if(pfs.type == BrokerPacket.BROKER_passid || pfs.type == BrokerPacket.BROKER_submitquery || pfs.type == BrokerPacket.BROKER_jobqueue)
						{
							System.out.println(pfs.symbol);
						}
						else
						{
							System.out.println("Unknown packet from JobTracker.");
						}
	
						/* re-print console prompt */
						System.out.print("> ");
					}
	
					/* tell server that i'm quitting */
					pts = new BrokerPacket();
					pts.type = BrokerPacket.BROKER_BYE;
					out.writeObject(pts);
			
					//Close channels and socket to broker
					out.close();
					in.close();
					stdIn.close();
					brokerSocket.close();
					System.exit(-1);
				} catch (IOException e) {
					//Try to get new address from JT and reconnect
		        	nodeCreatedSignal=new CountDownLatch(1);
		        	exists=null;
					break;
				} catch (ClassNotFoundException e) {
					//Try to get new address from JT and reconnect
		        	nodeCreatedSignal=new CountDownLatch(1);
		        	exists=null;
					break;
				}
			}
        }
	}
}
