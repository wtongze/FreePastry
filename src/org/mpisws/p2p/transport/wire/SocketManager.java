/*******************************************************************************

 "FreePastry" Peer-to-Peer Application Development Substrate

 Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
 for Software Systems.  All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are
 met:

 - Redistributions of source code must retain the above copyright
 notice, this list of conditions and the following disclaimer.

 - Redistributions in binary form must reproduce the above copyright
 notice, this list of conditions and the following disclaimer in the
 documentation and/or other materials provided with the distribution.

 - Neither the name of Rice  University (RICE), Max Planck Institute for Software
 Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
 promote products derived from this software without specific prior written
 permission.

 This software is provided by RICE, MPI-SWS and the contributors on an "as is"
 basis, without any representations or warranties of any kind, express or implied
 including, but not limited to, representations or warranties of
 non-infringement, merchantability or fitness for a particular purpose. In no
 event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
 incidental, special, exemplary, or consequential damages (including, but not
 limited to, procurement of substitute goods or services; loss of use, data, or
 profits; or business interruption) however caused and on any theory of
 liability, whether in contract, strict liability, or tort (including negligence
 or otherwise) arising in any way out of the use of this software, even if
 advised of the possibility of such damage.
 *******************************************************************************/
package org.mpisws.p2p.transport.wire;

import org.mpisws.p2p.transport.*;
import rice.environment.logging.Logger;
import rice.selector.SelectionKeyHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SocketManager extends SelectionKeyHandler implements P2PSocket<InetSocketAddress>, SocketRequestHandle<InetSocketAddress> {

    // the key to read from
    protected SelectionKey key;

    // the channel we are associated with
    protected AsynchronousSocketChannel channel;

    // the timer we use to check for stalled nodes
    protected rice.selector.TimerTask timer;

    protected TCPLayer tcp;

    Logger logger;

    InetSocketAddress addr;

    Map<String, Object> options;

    protected P2PSocketReceiver<InetSocketAddress> reader, writer;

    /**
     * becomes true before we deliver this to the SocketCallback, this invalidates the cancel() operation
     */
    boolean delivered = false;

    /**
     * Constructor which accepts an incoming connection, represented by the
     * selection key. This constructor builds a new SocketManager, and waits
     * until the greeting message is read from the other end. Once the greeting
     * is received, the manager makes sure that a socket for this handle is not
     * already open, and then proceeds as normal.
     *
     * @throws IOException DESCRIBE THE EXCEPTION
     */
    public SocketManager(TCPLayer tcp, AsynchronousSocketChannel ch) throws IOException {
        this.tcp = tcp;
        logger = tcp.logger;

        ch.setOption(StandardSocketOptions.SO_SNDBUF, tcp.SOCKET_BUFFER_SIZE);
        ch.setOption(StandardSocketOptions.SO_RCVBUF, tcp.SOCKET_BUFFER_SIZE);
        ch.setOption(StandardSocketOptions.TCP_NODELAY, tcp.TCP_NO_DELAY);

        addr = (InetSocketAddress) ch.getRemoteAddress();

        if (logger.level <= Logger.FINE) logger.log("(SA) " + "Accepted incoming connection from " + addr);

//        key = tcp.wire.environment.getSelectorManager().register(channel, this, 0);
    }

    /**
     * Constructor which creates an outgoing connection to the given node
     * handle using the provided address as a source route intermediate node.
     * This creates the connection by building the socket and sending
     * across the greeting message. Once the response greeting message is
     * received, everything proceeds as normal.
     *
     * @throws IOException An error
     */
    public SocketManager(final TCPLayer tcp, final InetSocketAddress addr, final SocketCallback<InetSocketAddress> c, Map<String, Object> options) throws IOException {
        this.tcp = tcp;
        this.options = options;
        logger = tcp.logger;
        this.addr = addr;

        channel = AsynchronousSocketChannel.open();
        channel.setOption(StandardSocketOptions.SO_SNDBUF, tcp.SOCKET_BUFFER_SIZE);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, tcp.SOCKET_BUFFER_SIZE);

        if (tcp.wire.forceBindAddress && tcp.wire.bindAddress != null)
            channel.bind(new InetSocketAddress(tcp.wire.bindAddress.getAddress(), 0));

        if (logger.level <= Logger.FINE) logger.log("(SM) Initiating socket connection to " + addr);

        channel.connect(addr, null, new CompletionHandler<>() {
            @Override
            public void completed(Void result, Object attachment) {
                delivered = true;
                if (logger.level <= Logger.FINE) logger.log("delivering " + SocketManager.this);
                tcp.wire.broadcastChannelOpened(addr, SocketManager.this.options, true);
                c.receiveResult(SocketManager.this, SocketManager.this);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                if (c == null) {
                    tcp.wire.errorHandler.receivedException(addr, exc);
                } else {
                    c.receiveException(SocketManager.this, new Exception(exc));
                }
            }
        });
    }

    public String toString() {
        return "SM " + addr + " " + channel;
    }

    /**
     * Method which closes down this socket manager, by closing the socket,
     * cancelling the key and setting the key to be interested in nothing
     */
//  Exception closeEx;
    public void close() {
//    logger.logException("Closing " + this, new Exception("Stack Trace"));
//    if (logger.level <= Logger.FINE) logger.log("close()");
        try {
            if (logger.level <= Logger.FINE) {
                logger.log("Closing " + this + " r:" + reader + " w:" + writer);
//        logger.log("Closing connection to " + addr);
            } else if (logger.level <= Logger.FINEST) {
                logger.logException("Closing " + this + " r:" + reader + " w:" + writer, new Exception("Stack Trace"));
            }

            if (key != null) {
//        closeEx = new Exception("Stack Trace");
                key.cancel();
                key.attach(null);
                key = null;
            } else {
                // we were already closed
                return;
            }

            if (channel != null) {
                channel.close();
            }
            tcp.socketClosed(this);


            tcp.wire.environment.getSelectorManager().invoke(new Runnable() {
                public void run() {
                    // notify the writer/reader because an intermediate layer may have closed the socket, and they need to know
                    if (writer != null) {
                        if (writer == reader) {
                            P2PSocketReceiver<InetSocketAddress> temp = writer;
                            writer = null;
                            reader = null;
                            temp.receiveException(SocketManager.this, new ClosedChannelException("Channel closed. " + SocketManager.this));
                        } else {
                            P2PSocketReceiver<InetSocketAddress> temp = writer;
                            writer = null;
                            temp.receiveException(SocketManager.this, new ClosedChannelException("Channel closed. " + SocketManager.this));
                        }
                    }

                    if (reader != null) {
                        if (tcp.isDestroyed()) return;
                        P2PSocketReceiver<InetSocketAddress> temp = reader;
                        reader = null;
                        temp.receiveException(SocketManager.this, new ClosedChannelException("Channel closed."));
                    }
                }
            });
        } catch (IOException e) {
            if (logger.level <= Logger.SEVERE) logger.log("ERROR: Recevied exception " + e + " while closing socket!");
        }
    }

    /**
     * The entry point for outgoing messages - messages from here are ensocketQueued
     * for transport to the remote node
     *
     * @param message DESCRIBE THE PARAMETER
     */
//  public Cancellable send(ByteBuffer message, int priority, Continuation<ByteBuffer, Exception> ack) {
//    Envelope e = new Envelope(message, priority, ack); 
//    pending.put(e);
//    tcp.wire.environment.getSelectorManager().modifyKey(key);
//    return e;
//  }

    /**
     * Method which should change the interestOps of the handler's key. This
     * method should *ONLY* be called by the selection thread in the context of
     * a select().
     *
     * @param key The key in question
     */
    public synchronized void modifyKey(SelectionKey key) {
        int flag = 0;
        if (reader != null) {
            flag |= SelectionKey.OP_READ;
        }
        if (writer != null) {
            flag |= SelectionKey.OP_WRITE;
        }
        key.interestOps(flag);
    }

    /**
     * Reads from the socket attached to this connector.
     *
     * @param key The selection key for this manager
     */
    public void read(SelectionKey key) {
        P2PSocketReceiver<InetSocketAddress> temp = null;
        synchronized (this) {
            if (reader == null) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                return;
            }
            temp = reader;
            reader = null;
        } // synchronized(this)
        try {
            temp.receiveSelectResult(this, true, false);
        } catch (IOException ioe) {
            temp.receiveException(this, ioe);
        }
        tcp.wire.environment.getSelectorManager().modifyKey(key);
    }

    /**
     * Writes to the socket attached to this socket manager.
     *
     * @param key The selection key for this manager
     */
    public void write(SelectionKey key) {
        P2PSocketReceiver<InetSocketAddress> temp = null;
        synchronized (this) {
            if (writer == null) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                return;
            }
            temp = writer;
//      clearTimer(writer);
            writer = null;
        }
        try {
            temp.receiveSelectResult(this, false, true);
        } catch (IOException ioe) {
            temp.receiveException(this, ioe);
        }
        tcp.wire.environment.getSelectorManager().modifyKey(key);
    }

    //  Exception regWriteEx;
//  long regWriteExTime;
    public synchronized void register(final boolean wantToRead, final boolean wantToWrite, P2PSocketReceiver<InetSocketAddress> receiver) {
        if (logger.level <= Logger.FINER)
            logger.log(this + ".register(" + (wantToRead ? "r" : "") + (wantToWrite ? "w" : "") + "," + receiver + ")");
        if (key == null) {
//      if (closeEx == null) {
//        logger.log("No closeEx "+addr);
//      } else {
//        logger.logException("closeEx "+addr, closeEx);
//      }
            ClosedChannelException cce = new ClosedChannelException("Socket " + addr + " " + SocketManager.this + " is already closed.");
            if (logger.level <= Logger.CONFIG)
                logger.logException("Socket " + addr + " " + this + " is already closed.", cce);
//            receiver.receiveException(this, cce);
            return;
        }
        // this check happens before setting the reader because we don't want to change any state if the exception is going ot be thrown
        // so don't put this check down below!
        if (wantToWrite) {
            if (!channel.isOpen()) {
                receiver.receiveException(this, new ClosedChannelException("Socket " + addr + " " + SocketManager.this + " already closed."));
                return;
            }
            if (writer != null) {
                if (writer != receiver) {
//          logger.logException("Already registered "+regWriteExTime,regWriteEx);
                    throw new IllegalStateException("Already registered " + writer + " for writing, you can't register " + receiver + " for writing as well! SM:" + this);
//          receiver.receiveException(this, 
//              new IOException(
//                  "Already registered "+writer+" for writing, you can't register "+receiver+" for writing as well!")); 
//          return;
                }
            }
        }
//    regWriteEx = new Exception("regWriteEx Stack Trace "+this);
//    regWriteExTime = tcp.wire.environment.getTimeSource().currentTimeMillis();

        if (wantToRead) {
            if (reader != null) {
                if (reader != receiver)
                    throw new IllegalStateException("Already registered " + reader + " for reading, you can't register " + receiver + " for reading as well!");
            }
            reader = receiver;
        }

        if (wantToWrite) {
            writer = receiver;
        }
        tcp.wire.environment.getSelectorManager().modifyKey(key);
    }

    /**
     * Method which initiates a shutdown of this socket by calling
     * shutdownOutput().  This has the effect of removing the manager from
     * the open list.
     */
    public void shutdownOutput() {
        boolean closeMe = false;
        synchronized (this) {
            if (key == null) {
                throw new IllegalStateException("Socket already closed.");
            }

            try {
                if (logger.level <= Logger.FINE) logger.log("Shutting down output on app connection " + this);

                channel.shutdownOutput();

                tcp.wire.environment.getSelectorManager().invoke(new Runnable() {
                    public void run() {
                        // notify the writer/reader because an intermediate layer may have closed the socket, and they need to know
                        if (writer != null) {
//                  try {
                            writer.receiveException(SocketManager.this, new ClosedChannelException("Channel shut down."));
                            //writer.receiveSelectResult(SocketManager.this, false, true);
//                  } catch (IOException e) {
//                    if (logger.level <= Logger.SEVERE) logger.log( "ERROR: Recevied exception " + e + " while closing socket!");
//                  }
                            writer = null;
                        }
                    }
                });

//          } else {
//            closeMe = true; 
//          }
//        } else
//          if (logger.level <= Logger.SEVERE) logger.log( "ERROR: Unable to shutdown output on channel; channel is null!");

//      } catch (SocketException e) {
//        if (logger.level <= Logger.FINE) logger.log( "ERROR: Received exception " + e + " while shutting down output for socket "+this);
//        closeMe = true;
            } catch (IOException e) {
                if (logger.level <= Logger.SEVERE)
                    logger.log("ERROR: Received exception " + e + " while shutting down output for socket " + this);
                closeMe = true;
            }
        } // synchronized(this)
        tcp.wire.environment.getSelectorManager().modifyKey(key);

        // close has it's own synchronization semantics, don't want to be holding a lock when calling
        if (closeMe) {
            close();
        }
    }

    public long read(ByteBuffer dst) {
        if (key == null || !channel.isOpen()) return -1;
        try {
            long ret = channel.read(dst).get();
            if (logger.level <= Logger.FINER) {
                if (logger.level <= Logger.FINEST) {
                    logger.log(this + "read(" + ret + "):" + Arrays.toString(dst.array()));
                } else {
                    logger.log(this + "read(" + ret + ")");
                }
            }
            tcp.notifyRead(ret, addr);
            return ret;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
//  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
//    //System.out.println(this+"read");
//    return channel.read(dsts, offset, length);
//  }

    public long write(ByteBuffer src) {
        if (key == null || !channel.isOpen()) return -1;
        try {
            long ret = channel.write(src).get();
            if (logger.level <= Logger.FINER) {
                if (logger.level <= Logger.FINEST) {
                    logger.log(this + "write(" + ret + "):" + Arrays.toString(src.array()));
                } else {
//          logger.logException(this+"write("+ret+")", new Exception("Stack Trace"));
                    logger.log(this + "write(" + ret + ")");
                }
            }
            tcp.notifyWrite(ret, addr);
            return ret;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean cancel() {
        if (key == null) return false;
        if (delivered) throw new IllegalStateException(this + ".cancel() Can't cancel, already delivered");
        close();
        return true;
    }

    public InetSocketAddress getIdentifier() {
        return addr;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public SocketChannel getSocketChannel() {
        tcp.wire.environment.getSelectorManager().cancel(key);
        return null;
    }

//  TreeSet<Envelope> pendingMessages;  
//  private int envSeq = Integer.MIN_VALUE;
//  class Envelope implements Comparable<Envelope>, Cancellable {
//    ByteBuffer message;
//    // for ordering
//    int priority;
//    int seq;
//    Continuation<ByteBuffer, Exception> ack;
//    
//    public Envelope(ByteBuffer message, int priority, Continuation<ByteBuffer, Exception> ack) {
//      this.message = message;
//      this.priority = priority;
//      this.ack = ack;
//      this.seq = envSeq++;
//    }
//
//    public int compareTo(Envelope that) {
//      int ret = that.priority - this.priority;
//      if (ret == 0)
//        ret = that.seq - this.seq;
//      return ret;
//    }
//
//    public boolean cancel() {
//      return pendingMessages.remove(this);
//    }
//  }
}
