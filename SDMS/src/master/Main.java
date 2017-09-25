package master;

import sdms.SDMSNode;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		SDMSNode smds = new SDMSNode("127.0.0.1", 5000, 5001, true);
//		smds.startServer();
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		String host = args[0];
		int broker_port = Integer.parseInt(args[1]);
		int port = Integer.parseInt(args[2]);
		boolean master = Boolean.valueOf(args[3]);
		SDMSNode smds1 = new SDMSNode(host, broker_port, port, master);
		smds1.startServer();
	}

}
