from cassandra.metadata import (Murmur3Token, Murmur3Token,
                                BytesToken, ReplicationStrategy,
                                NetworkTopologyStrategy, SimpleStrategy,
                                LocalStrategy, protect_name,
                                protect_names, protect_value, is_valid_name,
                                UserType, KeyspaceMetadata, get_schema_parser,
                                _UnknownStrategy, ColumnMetadata, TableMetadata,
                                IndexMetadata, Function, Aggregate,
                                Metadata, TokenMap)
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host

token_to_host_owner = {}

host1 = Host('1.0.0.0', SimpleConvictionPolicy)
host1.set_location_info('dc1', 'rack1')
host2 = Host('2.0.0.0', SimpleConvictionPolicy)
host2.set_location_info('dc2', 'rack2')
host3 = Host('3.0.0.0', SimpleConvictionPolicy)
host3.set_location_info('dc1', 'rack1')
host4 = Host('4.0.0.0', SimpleConvictionPolicy)
host4.set_location_info('dc2', 'rack2')

token_to_host_owner[Murmur3Token("-9000000000000000000")] = host1
token_to_host_owner[Murmur3Token("-6000000000000000000")] = host2
token_to_host_owner[Murmur3Token("4000000000000000000")] = host3
token_to_host_owner[Murmur3Token("9000000000000000000")] = host4

ring = [
        Murmur3Token("-9000000000000000000"),
        Murmur3Token("-6000000000000000000"),
        Murmur3Token("4000000000000000000"),
        Murmur3Token("9000000000000000000")
        ]

nts = NetworkTopologyStrategy({'dc1': 1, 'dc2': 1})
replica_map = nts.make_token_replica_map(token_to_host_owner, ring)

for token in replica_map:
    print token.value, "[",
    for replica in replica_map[token]:
        print replica.endpoint._address, " ",
    print "]"

