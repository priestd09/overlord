package proxy

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const exampleCluster = `
[[clusters]]
# This be used to specify the name of cache cluster.
name = "test-mc"
# The name of the hash function. Possible values are: sha1.
hash_method = "fnv1a_64"
# The key distribution mode. Possible values are: ketama.
hash_distribution = "ketama"
# A two character string that specifies the part of the key used for hashing. Eg "{}".
hash_tag = ""
# cache type: memcache | memcache_binary | redis | redis_cluster
cache_type = "memcache"
# proxy listen proto: tcp | unix
listen_proto = "tcp"
# proxy listen addr: tcp addr | unix sock path
listen_addr = "0.0.0.0:21211"
# Authenticate to the Redis server on connect.
redis_auth = ""
# The dial timeout value in msec that we wait for to establish a connection to the server. By default, we wait indefinitely.
dial_timeout = 1000
# The read timeout value in msec that we wait for to receive a response from a server. By default, we wait indefinitely.
read_timeout = 1000
# The write timeout value in msec that we wait for to write a response to a server. By default, we wait indefinitely.
write_timeout = 1000
# The number of connections that can be opened to each server. By default, we open at most 1 server connection.
node_connections = 2
# The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject is set to true. Defaults to 3.
ping_fail_limit = 3
# A boolean value that controls if server should be ejected temporarily when it fails consecutively ping_fail_limit times.
ping_auto_eject = false
# A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Also you can use alias name like: ip:port:weight alias.
servers = [
    "127.0.0.1:11211:1",
]

[[clusters]]
# This be used to specify the name of cache cluster.
name = "test-redis"
# The name of the hash function. Possible values are: sha1.
hash_method = "fnv1a_64"
# The key distribution mode. Possible values are: ketama.
hash_distribution = "ketama"
# A two character string that specifies the part of the key used for hashing. Eg "{}".
hash_tag = ""
# cache type: memcache | memcache_binary | redis | redis_cluster
cache_type = "redis"
# proxy listen proto: tcp | unix
listen_proto = "tcp"
# proxy listen addr: tcp addr | unix sock path
listen_addr = "0.0.0.0:26379"
# Authenticate to the Redis server on connect.
redis_auth = ""
# The dial timeout value in msec that we wait for to establish a connection to the server. By default, we wait indefinitely.
dial_timeout = 1000
# The read timeout value in msec that we wait for to receive a response from a server. By default, we wait indefinitely.
read_timeout = 1000
# The write timeout value in msec that we wait for to write a response to a server. By default, we wait indefinitely.
write_timeout = 1000
# The number of connections that can be opened to each server. By default, we open at most 1 server connection.
node_connections = 2
# The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject is set to true. Defaults to 3.
ping_fail_limit = 3
# A boolean value that controls if server should be ejected temporarily when it fails consecutively ping_fail_limit times.
ping_auto_eject = false
# A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Also you can use alias name like: ip:port:weight alias.
servers = [
    "127.0.0.1:6379:1",
]

[[clusters]]
# This be used to specify the name of cache cluster.
name = "test-redis-cluster"
# The name of the hash function. Possible values are: sha1.
hash_method = "fnv1a_64"
# The key distribution mode. Possible values are: ketama.
hash_distribution = "ketama"
# A two character string that specifies the part of the key used for hashing. Eg "{}".
hash_tag = "{}"
# cache type: memcache | memcache_binary | redis | redis_cluster
cache_type = "redis_cluster"
# proxy listen proto: tcp | unix
listen_proto = "tcp"
# proxy listen addr: tcp addr | unix sock path
listen_addr = "0.0.0.0:27000"
# Authenticate to the Redis server on connect.
redis_auth = ""
# The dial timeout value in msec that we wait for to establish a connection to the server. By default, we wait indefinitely.
dial_timeout = 1000
# The read timeout value in msec that we wait for to receive a response from a server. By default, we wait indefinitely.
read_timeout = 1000
# The write timeout value in msec that we wait for to write a response to a server. By default, we wait indefinitely.
write_timeout = 1000
# The number of connections that can be opened to each server. By default, we open at most 1 server connection.
node_connections = 2
# The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject is set to true. Defaults to 3.
ping_fail_limit = 3
# A boolean value that controls if server should be ejected temporarily when it fails consecutively ping_fail_limit times.
ping_auto_eject = false
# A list of server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Also you can use alias name like: ip:port:weight alias.
servers = [
    "127.0.0.1:7000:1 abc",
    "127.0.0.1:7001:2",
]
`

func TestClusterConfigLoadFromFile(t *testing.T) {
	path := "/tmp/overlord-proxy-test-example.toml"
	fd, err := os.Create(path)
	if !assert.NoError(t, err) {
		return
	}

	_, err = fd.WriteString(exampleCluster)
	if !assert.NoError(t, err) {
		return
	}
	assert.NoError(t, fd.Close())

	ccs := &ClusterConfigs{}
	err = ccs.LoadFromFile(path)
	assert.NoError(t, err)
	assert.Len(t, ccs.Clusters, 3)
}
