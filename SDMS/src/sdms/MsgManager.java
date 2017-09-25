package sdms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MsgManager {
	JSONObject message;

	private Logger logger = Logger.getLogger(MsgManager.class.getName());

	public JSONObject decodeMsg(String msg) {
		JSONParser parser = new JSONParser();
		JSONObject j_data = null;

		try {
			Object data = parser.parse(msg);
			j_data = (JSONObject) data;
		} catch (ParseException pe) {

		}

		return j_data;
	}

	public String encodeMsg(JSONObject msg) {
		StringWriter out = new StringWriter();
		try {
			msg.writeJSONString(out);
			String return_string = out.toString();
			return return_string;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}

	public void sendMultipleWithoutSocket(ArrayList<JSONObject> list,
			JSONObject message) {
		ArrayList<JSONObject> new_list = new ArrayList<JSONObject>();
		Iterator<JSONObject> iter = list.iterator();
//		this.logger.info("In Send Multi + " + list.toString());
		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			JSONObject new_json = new JSONObject();
			try {
				Socket sock = new Socket((String) temp.get("ip"),
						((Number) temp.get("port")).intValue());
				new_json.remove("socket");
				new_json.put("socket", sock);
				new_list.add(new_json);

			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		this.sendMultiple(new_list, message);
	}

	public void sendMultiple(ArrayList<JSONObject> list, JSONObject message) {

//		this.logger.info("Send to Multiple");
		Iterator<JSONObject> iter = list.iterator();
		while (iter.hasNext()) {
			JSONObject temp = iter.next();
			Socket sock = (Socket) temp.get("socket");
//			this.logger.info("send message" + message.toJSONString());
			try {
//				this.logger.info(temp.toJSONString());
				this.sendMsg(message, sock);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public JSONObject receiveMsg(Socket socket) throws IOException,
			SocketTimeoutException {
		// socket.setSoTimeout(30000);
		InputStream in = socket.getInputStream();
		DataInputStream dis = new DataInputStream(in);
		String msg = dis.readUTF();
		return this.decodeMsg(msg);
	}

	public void sendMsg(JSONObject json, Socket socket) throws IOException {
//		this.logger.info(json.toJSONString());
		OutputStream out = socket.getOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		dos.writeUTF(this.encodeMsg(json));
		out.flush();
		// this.logger.info("send message : " + json.toJSONString());

	}
}
