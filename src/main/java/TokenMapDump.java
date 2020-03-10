import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.token.*;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenMapDump {

    private static final String DC1 = "DC1";
    private static final String DC2 = "DC2";
    private static final String RACK1 = "RACK1";
    private static final String RACK2 = "RACK2";

    private static final CqlIdentifier KS1 = CqlIdentifier.fromInternal("ks1");
    private static final CqlIdentifier KS2 = CqlIdentifier.fromInternal("ks2");

    private static final TokenFactory TOKEN_FACTORY = new Murmur3TokenFactory();

    private static final ImmutableMap<String, String> REPLICATE_ON_BOTH_DCS =
            ImmutableMap.of(
                    "class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DC1, "1", DC2, "1");

    private static final String TOKENS[] = {
            "-9000000000000000000",
            "-6000000000000000000",
            "4000000000000000000",
            "9000000000000000000"
    };

    public static void main(String args[]) throws UnknownHostException {
        driverTokenMapping();
        cassandraTokenMapping();
    }

    public static void driverTokenMapping() {
        InternalDriverContext context = mock(InternalDriverContext.class);
        ReplicationStrategyFactory replicationStrategyFactory = new DefaultReplicationStrategyFactory(context);


        Node node1 = mockNode(DC1, RACK1, "1.0.0.0", ImmutableSet.of(TOKENS[0]));
        Node node2 = mockNode(DC2, RACK2, "2.0.0.0", ImmutableSet.of(TOKENS[1]));
        Node node3 = mockNode(DC1, RACK1, "3.0.0.0", ImmutableSet.of(TOKENS[2]));
        Node node4 = mockNode(DC2, RACK2, "4.0.0.0", ImmutableSet.of(TOKENS[3]));
        List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);

        List<KeyspaceMetadata> keyspaces =
                ImmutableList.of(
                        mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS));

        DefaultTokenMap tokenMap =
                DefaultTokenMap.build(nodes, keyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");

        System.out.println("Driver:");
        for (TokenRange token : tokenMap.getTokenRanges()) {
            Set<Node> replicas = tokenMap.getReplicas(KS1, token);
            System.out.printf("%20d [ ", ((Murmur3Token)token.getStart()).getValue());
            for (Node replica : replicas) {
                System.out.printf("%s ", replica);
            }
            System.out.println("]");
        }
    }

    public static void cassandraTokenMapping() throws UnknownHostException {
        DatabaseDescriptor.daemonInitialization();

        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        TokenMetadata metadata = new TokenMetadata();

        // NOTE: Topology defined in: resources/cassandra-topology.properties

        addToken(metadata, TOKENS[0], new byte[]{ 1, 0, 0, 0 });
        addToken(metadata, TOKENS[1], new byte[]{ 2, 0, 0, 0 });
        addToken(metadata, TOKENS[2], new byte[]{ 3, 0, 0, 0 });
        addToken(metadata, TOKENS[3], new byte[]{ 4, 0, 0, 0 });

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC1", "1");
        configOptions.put("DC2", "1");

        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy("test", metadata, snitch, configOptions);

        System.out.println("Cassandra:");
        for (String token : TOKENS) {
            List<InetAddress> replicas = strategy.calculateNaturalEndpoints(new Murmur3Partitioner.LongToken(Long.parseLong(token)), metadata);
            System.out.printf("%20s [ ", token);
            for (InetAddress replica : replicas) {
                System.out.printf("%s ", replica);
            }
            System.out.println("]");
        }
    }

    public static void addToken(TokenMetadata metadata, String token, byte[] bytes) throws UnknownHostException
    {
        Token token1 = new Murmur3Partitioner.LongToken(Long.parseLong(token));
        InetAddress addr1 = InetAddress.getByAddress(bytes);
        metadata.updateNormalToken(token1, addr1);
    }

    private static DefaultNode mockNode(String dc, String rack, String host, Set<String> tokens) {
        DefaultNode node = mock(DefaultNode.class);
        when(node.toString()).thenReturn(host);
        when(node.getDatacenter()).thenReturn(dc);
        when(node.getRack()).thenReturn(rack);
        when(node.getRawTokens()).thenReturn(tokens);
        return node;
    }

    private static KeyspaceMetadata mockKeyspace(CqlIdentifier name, Map<String, String> replicationConfig) {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getName()).thenReturn(name);
        when(keyspace.getReplication()).thenReturn(replicationConfig);
        return keyspace;
    }
}
