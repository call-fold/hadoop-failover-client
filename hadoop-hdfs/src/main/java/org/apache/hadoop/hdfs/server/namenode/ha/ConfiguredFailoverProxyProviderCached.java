package org.apache.hadoop.hdfs.server.namenode.ha;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

public class ConfiguredFailoverProxyProviderCached<T>
        extends AbstractNNFailoverProxyProvider<T> {
    private static final Log LOG = LogFactory.getLog(ConfiguredFailoverProxyProviderCached.class);
    protected final Configuration conf;

    static abstract interface ProxyFactory<T> {
        public abstract T createProxy(Configuration paramConfiguration, InetSocketAddress paramInetSocketAddress, Class<T> paramClass, UserGroupInformation paramUserGroupInformation, boolean paramBoolean, AtomicBoolean paramAtomicBoolean)
                throws IOException;
    }

    static class DefaultProxyFactory<T>
            implements ConfiguredFailoverProxyProviderCached.ProxyFactory<T> {
        public T createProxy(Configuration conf, InetSocketAddress nnAddr, Class<T> xface, UserGroupInformation ugi, boolean withRetries, AtomicBoolean fallbackToSimpleAuth)
                throws IOException {
            return (T) NameNodeProxies.createNonHAProxy(conf, nnAddr, xface, ugi, false, fallbackToSimpleAuth).getProxy();
        }
    }

    protected final List<AddressRpcProxyPair<T>> proxies = new ArrayList();
    private final UserGroupInformation ugi;
    protected final Class<T> xface;
    private int currentProxyIndex = 0;
    private final ProxyFactory<T> factory;

    public ConfiguredFailoverProxyProviderCached(Configuration conf, URI uri, Class<T> xface) {
        this(conf, uri, xface, new DefaultProxyFactory());
    }

    @VisibleForTesting
    ConfiguredFailoverProxyProviderCached(Configuration conf, URI uri, Class<T> xface, ProxyFactory<T> factory) {
        Preconditions.checkArgument(xface.isAssignableFrom(NamenodeProtocols.class), "Interface class %s is not a valid NameNode protocol!");

        this.xface = xface;

        this.conf = new Configuration(conf);
        int maxRetries = this.conf.getInt("dfs.client.failover.connection.retries", 0);

        this.conf.setInt("ipc.client.connect.max.retries", maxRetries);

        int maxRetriesOnSocketTimeouts = this.conf.getInt("dfs.client.failover.connection.retries.on.timeouts", 0);

        this.conf.setInt("ipc.client.connect.max.retries.on.timeouts", maxRetriesOnSocketTimeouts);
        try {
            this.ugi = UserGroupInformation.getCurrentUser();

            Map<String, Map<String, InetSocketAddress>> map = DFSUtil.getHaNnRpcAddresses(conf);

            Map<String, InetSocketAddress> addressesInNN = (Map) map.get(uri.getHost());
            if ((addressesInNN == null) || (addressesInNN.size() == 0)) {
                throw new RuntimeException("Could not find any configured addresses for URI " + uri);
            }
            Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
            for (InetSocketAddress address : addressesOfNns) {
                this.proxies.add(new AddressRpcProxyPair(address));
            }
            HAUtil.cloneDelegationTokenForLogicalUri(this.ugi, uri, addressesOfNns);
            this.factory = factory;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Class<T> getInterface() {
        return this.xface;
    }

    public synchronized ProxyInfo<T> getProxy() {
        this.currentProxyIndex = ConfiguredFailoverProxyProviderCachedIndex.getIndex();
        AddressRpcProxyPair<T> current = (AddressRpcProxyPair) this.proxies.get(this.currentProxyIndex);
        if (current.namenode == null) {
            try {
                current.namenode = this.factory.createProxy(this.conf, current.address, this.xface, this.ugi, false, this.fallbackToSimpleAuth);
            } catch (IOException e) {
                LOG.error("Failed to create RPC proxy to NameNode", e);
                throw new RuntimeException(e);
            }
        }
        return new ProxyInfo(current.namenode, current.address.toString());
    }

    public void performFailover(T currentProxy) {
        incrementProxyIndex();
    }

    synchronized void incrementProxyIndex() {
        this.currentProxyIndex = ((this.currentProxyIndex + 1) % this.proxies.size());
        ConfiguredFailoverProxyProviderCachedIndex.setIndex(this.currentProxyIndex);
    }

    private static class AddressRpcProxyPair<T> {
        public final InetSocketAddress address;
        public T namenode;

        public AddressRpcProxyPair(InetSocketAddress address) {
            this.address = address;
        }
    }

    public synchronized void close()
            throws IOException {
        for (AddressRpcProxyPair<T> proxy : this.proxies) {
            if (proxy.namenode != null) {
                if ((proxy.namenode instanceof Closeable)) {
                    ((Closeable) proxy.namenode).close();
                } else {
                    RPC.stopProxy(proxy.namenode);
                }
            }
        }
    }

    public boolean useLogicalURI() {
        return true;
    }
}
