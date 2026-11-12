# Unix Domain Sockets

`transport = "unix"` opens each connection to the stream socket at the path in
`target`, such as `"/var/run/redis/redis.sock"`. Any protocol that runs over
`tcp` runs over it.

Unix sockets have no kernel or NIC timestamps, so `timestamps` must be
`"user"`, the default.
