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

import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.*;

public class TCPLayer {
    public static final Map<String, Object> OPTIONS;

    static {
        Map<String, Object> map = new HashMap<>();
        map.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_GUARANTEED);
        OPTIONS = Collections.unmodifiableMap(map);
    }

    // the number of sockets where we start closing other sockets
    public final int MAX_OPEN_SOCKETS;

    // the size of the buffers for the socket
    public final int SOCKET_BUFFER_SIZE;
    public boolean TCP_NO_DELAY = false;

    private AsynchronousServerSocketChannel channel;

    WireTransportLayerImpl wire;

    Logger logger;

    public TCPLayer(WireTransportLayerImpl wire, boolean enableServer) throws IOException {
        this.wire = wire;
        this.logger = wire.environment.getLogManager().getLogger(TCPLayer.class, null);

        Parameters p = wire.environment.getParameters();
        MAX_OPEN_SOCKETS = p.getInt("pastry_socket_scm_max_open_sockets");
        SOCKET_BUFFER_SIZE = p.getInt("pastry_socket_scm_socket_buffer_size"); // 32768
        if (p.contains("transport_tcp_no_delay")) {
            TCP_NO_DELAY = p.getBoolean("transport_tcp_no_delay");
        }

        // bind to port
        if (enableServer) {
            try (AsynchronousServerSocketChannel channel = AsynchronousServerSocketChannel.open()) {
                channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                channel.bind(wire.bindAddress);
                this.channel = channel;
                System.out.println(">>>>> TCP Layer");
            }
            logger.log("TCPLayer bound to " + wire.bindAddress);

            // this.key = wire.environment.getSelectorManager().register(channel, this, SelectionKey.OP_ACCEPT);
            channel.accept(null, new CompletionHandler<>() {
                @Override
                public void completed(AsynchronousSocketChannel ch, Object att) {
                    channel.accept(null, this);
                    accept(ch);
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                }
            });
        }
    }

    public SocketRequestHandle<InetSocketAddress> openSocket(
            InetSocketAddress destination,
            SocketCallback<InetSocketAddress> deliverSocketToMe,
            Map<String, Object> options) {
        if (isDestroyed()) return null;
        if (logger.level <= Logger.FINEST) {
            logger.logException("openSocket(" + destination + ")", new Exception("Stack Trace"));
        } else {
            if (logger.level <= Logger.FINE) logger.log("openSocket(" + destination + ")");
        }
        if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");
        try {
            wire.broadcastChannelOpened(destination, options, true);

            synchronized (sockets) {
                SocketManager sm = new SocketManager(this, destination, deliverSocketToMe, options);
                sockets.add(sm);
                return sm;
            }
        } catch (IOException e) {
            if (logger.level <= Logger.WARNING)
                logger.logException("GOT ERROR " + e + " OPENING PATH - MARKING PATH " + destination + " AS DEAD!", e);
            SocketRequestHandle<InetSocketAddress> can = new SocketRequestHandleImpl<InetSocketAddress>(destination, options, logger);
            deliverSocketToMe.receiveException(can, e);
            return can;
        }
    }

    final Collection<SocketManager> sockets = new HashSet<SocketManager>();

    protected void socketClosed(SocketManager sm) {
        wire.broadcastChannelClosed(sm.addr, sm.options);
        sockets.remove(sm);
    }

    public void destroy() {
        if (logger.level <= Logger.INFO) logger.log("destroy()");

        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ioe) {
            wire.errorHandler.receivedException(null, ioe);
        }

        // TODO: add a flag to disable this to simulate a silent fault
        for (SocketManager socket : new ArrayList<SocketManager>(sockets)) {
            // logger.log("closing "+socket);
            socket.close();
        }
    }

    public void acceptSockets(final boolean b) {
    }

    /**
     * Specified by the SelectionKeyHandler interface. Is called whenever a key
     * has become acceptable, representing an incoming connection. This method
     * will accept the connection, and attach a SocketConnector in order to read
     * the greeting off of the channel. Once the greeting has been read, the
     * connector will hand the channel off to the appropriate node handle.
     */
    public void accept(AsynchronousSocketChannel channel) {
        try {
            SocketManager sm = new SocketManager(this, channel);
            synchronized (sockets) {
                sockets.add(sm);
            }
            wire.incomingSocket(sm);
        } catch (IOException e) {
            if (logger.level <= Logger.WARNING) logger.log("ERROR (accepting connection): " + e);
            wire.errorHandler.receivedException(null, e);
        }
    }

    public boolean isDestroyed() {
        return wire.isDestroyed();
    }

    public void notifyRead(long ret, InetSocketAddress addr) {
        wire.notifyRead(ret, addr, true);
    }

    public void notifyWrite(long ret, InetSocketAddress addr) {
        wire.notifyWrite(ret, addr, true);
    }
}
