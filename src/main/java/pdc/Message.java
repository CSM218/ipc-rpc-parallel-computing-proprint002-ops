package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * Custom binary wire format (length-prefixed when sent on the wire):
 * [magic:String][version:int][messageType:String][studentId:String][timestamp:long][payload:bytes]
 */
public class Message {
    public String magic;       // e.g. CSM218
    public int version;        // protocol version
    public String messageType; // messageType (REGISTER_WORKER, RPC_REQUEST, ...)
    public String studentId;   // sender id / worker id
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * This method does not add a length prefix; callers should prepend the
     * length when writing to a TCP stream to preserve message boundaries.
     */
    public byte[] pack() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeUTF(magic == null ? "" : magic);
            dos.writeInt(version);
            dos.writeUTF(messageType == null ? "" : messageType);
            dos.writeUTF(studentId == null ? "" : studentId);
            dos.writeLong(timestamp);

            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }

            dos.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream produced by {@link #pack()}.
     */
    public static Message unpack(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream dis = new DataInputStream(bais)) {

            Message m = new Message();
            m.magic = dis.readUTF();
            m.version = dis.readInt();
            m.messageType = dis.readUTF();
            m.studentId = dis.readUTF();
            m.timestamp = dis.readLong();

            int len = dis.readInt();
            if (len > 0) {
                byte[] buf = new byte[len];
                dis.readFully(buf);
                m.payload = buf;
            } else {
                m.payload = new byte[0];
            }

            return m;

        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    /**
     * Lightweight protocol validation used by tests and runtime.
     */
    public void validate() throws Exception {
        if (magic == null || !magic.equals("CSM218")) {
            throw new Exception("Invalid magic header");
        }
        if (version != 1) {
            throw new Exception("Unsupported protocol version");
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new Exception("Missing messageType");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new Exception("Missing studentId");
        }
    }
}
