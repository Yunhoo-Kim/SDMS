package slave;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

import org.json.simple.JSONObject;

import sdms.MsgManager;
import sdms.SDMSNode;

public class SlaveListener implements Runnable {
	Socket socket;
	MsgManager msg_manager = new MsgManager();

	public SlaveListener(Socket sock) {
		this.socket = sock;
	}

	public void receiveMsg(JSONObject json) {
		System.out.println("MSG " + json.toJSONString());
		SlaveWorker worker = new SlaveWorker(json, this);
		worker.workerExecutor();
	}

	public JSONObject receiveMsg(Socket socket) {
		try {
			
			return this.msg_manager.receiveMsg(socket);
		} catch (IOException io) {
			io.printStackTrace();
		}
		return new JSONObject();
	}

	public void sendMsg(JSONObject json) {
		try {
			this.msg_manager.sendMsg(json, this.socket);
		} catch (IOException e) {
			SlaveServer slave = SlaveServer.getInstance();
			slave.removeClient(this.socket);
		}
	}

	public void sendMsg(JSONObject json, Socket socket) {
		try {
			this.msg_manager.sendMsg(json, socket);
		} catch (IOException io) {

		}
	}

	public void sendMultiple(ArrayList<JSONObject> list, JSONObject message) {
		this.msg_manager.sendMultiple(list, message);
	}

	public void run() {
		try {
			while (!Thread.currentThread().isInterrupted()) {
				this.receiveMsg(this.msg_manager.receiveMsg(this.socket));
			}
			SlaveServer.getInstance().logger.info("client disconnect");
			this.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			SlaveServer server = SlaveServer.getInstance();
			server.removeClient(this.socket);
			server.client_threads.remove(Thread.currentThread());
		}

	}

}
