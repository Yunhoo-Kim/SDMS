package slave;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Timer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import sdms.HeartbeatManager;
import sdms.MsgManager;
import sdms.SDMSNode;

public class SlaveServer implements Runnable {
	ArrayList<JSONObject> client_table = new ArrayList<JSONObject>();
	ArrayList<Thread> election_threads = new ArrayList<Thread>();
	ArrayList<Thread> client_threads = new ArrayList<Thread>();
	ArrayList<Socket> client_sockets = new ArrayList<Socket>();
	Thread master_thread;
	boolean eligible;
	boolean election_flag = false;
	public boolean election_end_flag = false;
	int election_cnt = 0;
	String master_ip;
	int master_id = 0;
	int master_port;

	public Logger logger = Logger.getLogger(String.valueOf(Thread
			.currentThread().getId()));

	public static SlaveServer slave_server;
	private SDMSNode sdms_node;

	public static SlaveServer getInstance() {
		return slave_server;
	}

	public SlaveServer() {
		slave_server = this;
		this.sdms_node = SDMSNode.getInstance();
	}

	public void run() {
		this.addToBroker();
		if(this.sdms_node.getMasterFlag())
			return;
		try {
			this.sdms_node.server_socket = new ServerSocket(
					this.sdms_node.getPort());

			while (!Thread.currentThread().isInterrupted()) {
				Socket client = this.sdms_node.server_socket.accept();
				this.logger.info("Client Accept" + "");
				Thread client_thread = new Thread(new SlaveListener(client));
				client_thread.start();
				this.client_threads.add(client_thread);
				this.client_sockets.add(client);
			}
			this.sdms_node.server_socket.close();
		} catch (IOException e) {
			e.printStackTrace();
			this.logger.info("Slave server Down!!!");
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public void setMaster(String ip, int port, int id) {
		this.master_id = id;
		this.master_ip = ip;
		this.master_port = port;
	}

	public void setEligible(boolean flag) {
		this.eligible = flag;
	}

	public boolean getEligible() {
		return this.eligible;
	}

	public void setElectionFlag(boolean flag) {
		this.election_flag = flag;
	}

	public boolean getElectionFlag() {
		return this.election_flag;
	}

	public void addTopic(int client_id, String topic) {
		Iterator<JSONObject> iter = this.client_table.iterator();

		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			if (((Number) temp.get("client_id")).intValue() == client_id) {
				ArrayList<String> topic_list = (ArrayList<String>) temp
						.getOrDefault("topic_list", new ArrayList<String>());
				topic_list.add(topic);
				temp.put("topic_list", topic_list);
			}
		}
	}

	public void removeTopic(int client_id, String topic) {
		Iterator<JSONObject> iter = this.client_table.iterator();

		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			if (((Number) temp.get("client_id")).intValue() == client_id) {
				ArrayList<String> topic_list = (ArrayList<String>) temp
						.getOrDefault("topic_list", new ArrayList<String>());
				topic_list.remove(topic);
				temp.put("topic_list", topic_list);
			}
		}
	}

	public ArrayList<JSONObject> getClientList() {
		return this.client_table;
	}

	public void addClient(Socket socket, int client_id) {
		JSONObject data = new JSONObject();
		data.put("socket", socket);
		data.put("client_id", client_id);
		this.client_table.add(data);
	}

	public void removeClient(int client_id) {
		Iterator<JSONObject> iter = this.client_table.iterator();

		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			if (((Number) temp.get("client_id")).intValue() == client_id) {
				this.client_table.remove(temp);
				break;
			}
		}
	}

	public void removeClient(Socket socket) {
		Iterator<JSONObject> iter = this.client_table.iterator();

		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			if (((Socket) temp.get("socket")) == socket) {
				this.client_table.remove(temp);
				break;
			}
		}
	}

	public void runAsMaster() {
		// this.logger.info("Start Running As Master "
		// + this.client_threads.size());
		for (int i = 0; i < this.client_threads.size(); i++) {
			this.logger
					.info("*********************Interrupt Client thread*********************");
			try {
				this.client_sockets.get(i).close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.client_threads.get(i).interrupt();
		}
		// this.client_threads
		try {
			this.sdms_node.heartbeat_timer.cancel();
		} catch (Exception e) {
			this.sdms_node.runAsMaster(this.master_id);
			return;
		}
		this.sdms_node.runAsMaster(this.master_id);
	}

	public void addToBroker() {
		try {
			Socket socket = new Socket(this.sdms_node.getBrokerIP(),
					this.sdms_node.getBrokerPort());
			SlaveListener listener = new SlaveListener(socket);
			JSONObject request_register = new JSONObject();
			request_register.put("op_type", "register server");
			request_register.put("node_type", "slave");
			listener.sendMsg(request_register);
			JSONObject response = listener.receiveMsg(socket);
			JSONObject master_address = (JSONObject) response
					.get("master_address");
			try{
				String master_ip = (String) master_address.get("ip");
				System.out.println(response.toJSONString());
				int master_id = ((Number) master_address.get("node_id"))
						.intValue();
				int master_port = ((Number) master_address.get("port"))
						.intValue();
				this.setMaster(master_ip, master_port, master_id);
				this.registerToMaster();
			}catch(NullPointerException e){
				this.runAsMaster();
			}

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void registerToMaster() {
		try {
			Socket socket = new Socket(this.master_ip, this.master_port);
			SlaveListener listener = new SlaveListener(socket);
			JSONObject request_register = new JSONObject();
			this.master_thread = new Thread(new MasterConnectionManager(socket));
			this.master_thread.start();
			request_register.put("op_type", "slave register");
			request_register.put("ip", this.sdms_node.getIP());
			request_register.put("node_id", this.sdms_node.getId());
			request_register.put("port", this.sdms_node.getPort());
			request_register.put("weight", this.sdms_node.getWeight());
			request_register.put("eligible", true);
			listener.sendMsg(request_register);
			this.sdms_node.startHeartbeatManager();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void startElection() {
		ArrayList<JSONObject> slave_list = this.sdms_node.getSlaveList();
		int cnt = 0;
		this.setElectionFlag(true);
		// initalize
		this.election_threads.clear();
		this.logger = Logger.getLogger(String.valueOf(Thread.currentThread()
				.getId()));
		this.logger.info(String.valueOf(Thread.currentThread().getId())
				+ " Slave List : " + this.sdms_node.getSlaveList().toString());
		for (int i = 0; i < slave_list.size(); i++) {
			JSONObject temp = slave_list.get(i);
			if (((Number) temp.get("node_id")).intValue() > this.sdms_node
					.getId()) {
				try {
					this.logger.info(((Number) temp.get("node_id")).intValue()
							+ " Has bully " + this.sdms_node.getId());
					if (this.election_cnt == 0) {
						Socket sock = new Socket((String) temp.get("ip"),
								((Number) temp.get("port")).intValue());
						Thread t = new Thread(new ElectionThread(sock));

						t.start();
						this.election_threads.add(t);

					} else {
						break;
					}
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		this.logger.info("Thread size is " + this.election_threads.size());
		// Iterator<Thread> e_thread = this.election_threads.iterator();
		for (int i = 0; i < this.election_threads.size(); i++) {
			Thread temp = this.election_threads.get(i);
			try {
				temp.join();
			} catch (Exception e) {
			}
		}

		if (this.election_cnt == 0) {
			this.logger.info(this.sdms_node.getId() + " is bully");
			this.sendBullyIsMe();
			// i am bully of the groups
		} else {
			while (!this.election_end_flag) {

			}
		}
	}

	public int getClientConnections() {
		return this.client_table.size();
	}

	private void sendBullyIsMe() {
		ArrayList<JSONObject> slave_list = this.sdms_node.getSlaveList();
		ArrayList<JSONObject> new_list = new ArrayList<JSONObject>();

		Iterator<JSONObject> iter = slave_list.iterator();

		while (iter.hasNext()) {
			JSONObject temp = (JSONObject) iter.next().clone();

			if (((Number) temp.get("node_id")).intValue() != this.sdms_node
					.getId()) {
				new_list.add(temp);
			}
		}

		JSONObject json = this.sdms_node.getMyInfo();
		json.put("op_type", "i am bully");
		this.sdms_node.setIsBully(true);
		this.runAsMaster();
		MsgManager msg_manager = new MsgManager();
		msg_manager.sendMultipleWithoutSocket(new_list, json);

	}

}
