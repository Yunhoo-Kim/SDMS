package master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import org.json.simple.JSONObject;

import queue.HeartBeatQueue;
import sdms.SDMSNode;

public class MasterServer implements Runnable {

	public static MasterServer master_server;
	private SDMSNode sdms_node;

	public static MasterServer getInstance() {
		return master_server;
	}

	public MasterServer() {
		master_server = this;
		sdms_node = SDMSNode.getInstance();
	}

	public void run() {
		if (!this.sdms_node.getIsBully())
			this.addToBroker();
		else
			this.sdms_node.heartbeat_thread.interrupt();
		
		this.sdms_node.startHeartbeatManager();
		try {
			this.sdms_node.server_socket = new ServerSocket(
					this.sdms_node.getPort());

			while (!Thread.currentThread().isInterrupted()) {
				Socket client = this.sdms_node.server_socket.accept();
				new Thread(new MasterListener(client)).start();
			}
			this.sdms_node.server_socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
	}

	public void addToBroker() {
		try {
			Socket socket = new Socket(this.sdms_node.getBrokerIP(),
					this.sdms_node.getBrokerPort());
			MasterListener listener = new MasterListener(socket);
			JSONObject register_master = new JSONObject();
			register_master.put("op_type", "register server");
			register_master.put("node_type", "master");
			register_master.put("ip", this.sdms_node.getIP());
			register_master.put("port", this.sdms_node.getPort());
			register_master.put("node_id", this.sdms_node.getId());
			register_master.put("weight", this.sdms_node.getWeight());
			listener.sendMsg(register_master);
			JSONObject response = listener.receiveMsg(socket);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public JSONObject loadBalancing() {
		ArrayList<JSONObject> temp = this.sdms_node.getSlaveList();
//		int node_id = 0;
		int node_index = 0;
		int min_connections = 1000000;
		
		for(int i=0;i<temp.size();i++){
			int temp_size = ((Number)temp.get(i).get("connections")).intValue();
			if(min_connections > temp_size){
//				node_id = ((Number)temp.get(i).get("node_id")).intValue();
				min_connections = temp_size;
				node_index = i;
			}
		}
		JSONObject slave = (JSONObject)temp.get(node_index).clone();
//		JSONObject slave = (JSONObject) temp.get(random.nextInt(temp.size()))
//				.clone();
		// System.out.println(slave.toJSONString());
		slave.remove("socket");
		return slave;
	}
}
