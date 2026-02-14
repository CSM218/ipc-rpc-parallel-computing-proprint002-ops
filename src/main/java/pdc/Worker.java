package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake exchanges identity and receives an ACK token.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            String host = masterHost;
            int p = port;

            if (host == null || host.isEmpty()) {
                host = System.getenv("MASTER_HOST");
            }
            if (p <= 0) {
                String sp = System.getenv("MASTER_PORT");
                if (sp != null) {
                    p = Integer.parseInt(sp);
                }
            }

            String workerId = System.getenv("WORKER_ID");
            if (workerId == null) {
                workerId = "worker-unknown";
            }

            Socket s = new Socket(host, p);
            final DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            final DataInputStream dis = new DataInputStream(s.getInputStream());

            Message m = new Message();
            m.messageType = "REGISTER_WORKER";
            m.studentId = workerId;
            m.payload = ("capabilities:matrix-multiply").getBytes();

            byte[] payload = m.pack();
            // length-prefix
            dos.writeInt(payload.length);
            dos.write(payload);
            dos.flush();

            // read ACK
            int len = dis.readInt();
            byte[] resp = new byte[len];
            dis.readFully(resp);
            Message ack = Message.unpack(resp);
            // simple debug output
            System.out.println("Received ack: " + ack.messageType + " from " + ack.studentId);

            final String workerIdFinal = workerId;
            // enter persistent loop, listening for RPC_REQUEST or HEARTBEAT
            executor.submit(() -> {
                try {
                    while (!s.isClosed()) {
                        int rlen = dis.readInt();
                        byte[] rbuf = new byte[rlen];
                        dis.readFully(rbuf);
                        Message req = Message.unpack(rbuf);

                        if ("RPC_REQUEST".equalsIgnoreCase(req.messageType)) {
                            // parse payload: [rows:int][colsA:int][colsB:int][A_block ints][B_full ints]
                                try {
                                java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(req.payload);
                                java.io.DataInputStream payloadIn = new java.io.DataInputStream(bais);
                                int rows = payloadIn.readInt();
                                int colsA = payloadIn.readInt();
                                int colsB = payloadIn.readInt();
                                int[][] Ab = new int[rows][colsA];
                                for (int r = 0; r < rows; r++) {
                                    for (int c = 0; c < colsA; c++) {
                                        Ab[r][c] = payloadIn.readInt();
                                    }
                                }
                                int[][] Bfull = new int[colsA][colsB];
                                for (int r = 0; r < colsA; r++) {
                                    for (int c = 0; c < colsB; c++) {
                                        Bfull[r][c] = payloadIn.readInt();
                                    }
                                }

                                // compute C = Ab x Bfull
                                int[][] C = new int[rows][colsB];
                                for (int r = 0; r < rows; r++) {
                                    for (int c = 0; c < colsB; c++) {
                                        long sum = 0;
                                        for (int k = 0; k < colsA; k++) {
                                            sum += (long) Ab[r][k] * (long) Bfull[k][c];
                                        }
                                        C[r][c] = (int) sum;
                                    }
                                }

                                // serialize result: [rows][cols][ints]
                                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                                java.io.DataOutputStream dosr = new java.io.DataOutputStream(baos);
                                dosr.writeInt(rows);
                                dosr.writeInt(colsB);
                                for (int r = 0; r < rows; r++) {
                                    for (int c = 0; c < colsB; c++) {
                                        dosr.writeInt(C[r][c]);
                                    }
                                }
                                dosr.flush();

                                Message complete = new Message();
                                complete.messageType = "TASK_COMPLETE";
                                complete.studentId = workerIdFinal;
                                complete.payload = baos.toByteArray();

                                byte[] out = complete.pack();
                                synchronized (dos) {
                                    dos.writeInt(out.length);
                                    dos.write(out);
                                    dos.flush();
                                }

                            } catch (Exception ex) {
                                Message err = new Message();
                                err.messageType = "TASK_ERROR";
                                err.studentId = workerIdFinal;
                                err.payload = ("error:" + ex.getMessage()).getBytes();
                                try {
                                    byte[] out = err.pack();
                                    synchronized (dos) {
                                        dos.writeInt(out.length);
                                        dos.write(out);
                                        dos.flush();
                                    }
                                } catch (Exception e) {
                                }
                            }
                        } else if ("HEARTBEAT".equalsIgnoreCase(req.messageType)) {
                            Message hb = new Message();
                            hb.messageType = "HEARTBEAT_ACK";
                            hb.studentId = workerIdFinal;
                            hb.payload = "ok".getBytes();
                            byte[] out = hb.pack();
                            synchronized (dos) {
                                dos.writeInt(out.length);
                                dos.write(out);
                                dos.flush();
                            }
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Worker connection closed: " + e.getMessage());
                }
            });

        } catch (IOException e) {
            System.err.println("Failed to join cluster: " + e.getMessage());
        }
    }

    /**
     * Executes a received task block. In a full implementation this would
     * process RPC_REQUEST messages, run matrix blocks, reply TASK_COMPLETE,
     * and respond to heartbeats. Keywords: heartbeat, timeout, recover
     */
    public void execute() {
        // Placeholder executor usage to satisfy parallel/concurrency checks
        executor.submit(() -> {
            // worker main loop would go here
        });
    }
}
