package slave;

import java.io.IOException;
import java.net.Socket;

import org.json.simple.JSONObject;

import sdms.MsgManager;
import sdms.SDMSNode;

public class ElectionThread implements Runnable {
	MsgManager msg_manager = new MsgManager();
	Socket socket;

	public ElectionThread(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		JSONObject json = new JSONObject();
		json.put("op_type", "start election");
		try {
			this.socket.setSoTimeout(30000);
			this.msg_manager.sendMsg(json, this.socket);
			SlaveServer slave = SlaveServer.getInstance();
			slave.election_cnt++;
			SDMSNode sdms = SDMSNode.getInstance();
			sdms.logger.info("client connected");
			this.socket.close();
		} catch (IOException io) {
			SDMSNode sdms = SDMSNode.getInstance();
			sdms.logger.info("Slave Server Does not answer");
		}
		
		SDMSNode.getInstance().logger.info("thread end");
	}
}
