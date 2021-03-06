Clusterd architecture
----

Clusterd is a system for managing services running over a set of
computers (known as a cluster).

Clusterd features:

* Automatic service scheduling with dependencies
* Automatic restart on service failures (configurable)
* Migration of running services
* Reduncancy of services across a set of configurable failure domains

Technical details:

* written in C

## How it works

Clusterd runs on a set of servers. There is no special server. All
servers are equal.

Every clusterd host is identified by a host key, a 512-bit identifier.

Clusterd runs processes on servers, according to a
configuration. Configurations can be added and removed
dynamically. When added, every configuration is given a namespace id,
which is a cluster-wide unique UUID.

## Requirements

To install and use clusterd, you need an existing dqlite setup and a
private network for secure intra-cluster communication.

## Starting

You start clusterd by running

```
clusterd dqlitehost:dqliteport --private-network <private-network-ip>
```

Clusterd should have permissions to access the `/var/lib/clusterd` (or
whatever path is in `CLUSTERD_HOME`). If the directory does not exist,
it will create it.

Inside this directory is the `/run` directory, which contains a
subdirectory for each namespace that has a process running on this
node.

Also in this directory is `/var/lib/clusterd/host` which contains
information on the host (usually a name and optional tags)

## Configuration

A clusterd specification is a directory containing subdirectories for
each service that needs to be scheduled on the cluster. Inside each
service subdirectory, there should be a file named `service` and a set of
executable files called hooks. These are the supported hooks:

* `prepare` -- this is a script that gets executed on a node before a
  service is started. It ought to install any dependencies that the
  host would need to have to run the service. If this script fails,
  clusterd will attempt to restart 10 times, or whatever the parameter
  `prepare.start_limit` is in the service file.
* `launch` -- this script is run after the prepare command finishes
  successfully. It should launch the service. While the service is
  running, it should send clusterd okay messages to the clusterd
  service on the local host according to the `CLUSTERD_` environment variables.

  When the launch process receives SIGINT, it should begin graceful
  shutdown procedures of the service. The service is given 2 minutes
  to shut down (or whatever the value of
  `launch.shutdown_grace_period` is). If the service does not die
  within that time, then a SIGQUIT message is sent. This instructs the
  service to begin immediate shutdown. A grace period of 30 seconds is
  applied (or whatever the value of
  `launch.abort_grace_period`). Finally, a SIGKILL message is
  generated, and a log entry is noted that the process failed to shut
  down.
* `finish` -- If launch dies without being instructed to, or if launch
  dies with exit code 1, then this script is launched to clean up any
  local files that need removal.
* `cleanup` -- Whne clusterd decides it doesn't want this service on
  this machine anymore, this script is called to trigger a removal of
  any mess that may have been made on the local system.

Only the `launch` script is necessary.

## Launching a configuration

To launch a configuration, run `clusterdctl`:

```
cluster-launch <path/to/configuration>
```

You can optionally name the new namespace:

```
cluster-launch <path/to/configuration> -L <name>
```

Or add optional metadata

```
cluster-launch <path/to/configuration> -L <name> -D var1=value -D var2=value -D flag
```

When you launch a cluster, `cluster-launch` opens the configuration
directory and validate it. Then it begins the scheduling process.

## Scheduling

Upon creation, a configuration enters the scheduling state. At this
point, the configuration is *not* available, and the clusterdctl
command is still running. The state is reflected in etcd. You can run
`cluster-find-stale` to determine clusters that have been
scheduling for some time.

To resume scheduling run `cluster-launch --resume <namespace-id>`.

Note that scheduling clusters may make progress once other nodes join
in.

As cluster-launch identifies hosts that can launch services in a
namespace, begins to contact those hosts, and request them to join the
service. When a node accepts joining a service, cluster-launch makes a
note in etcd (a lease) and then runs `cluster-launch --resume
<namespace id>` on that node via ssh. This causes the node to download
a copy of the cluster specification, and begin launching.

Upon a resumption, the node examines the etcd state to determine if it
should attempt to start up any services. It may take over a lease to
do so.

When a node decides to start a service, it launches a
`cluster-service` process with the namespace id and service name and
instance id as arguments. Additionally, some number of nodes are
marked as 'monitors' for the service. Monitors are chosen based on
failure domains. They live throughout the cluster and ensure this
service stays up cluster-wide, and is accessible from all nodes. The
chosen monitors are also given to the cluster-service command
line. The monitors do not receive notification they are monitors.

The `cluster-service` process will periodically send heartbeats to at
least one node, and will respond to heartbeat acks.

## Monitors

All nodes in the cluster run a `cluster-monitor` process that
processes monitor requests. Monitor requests are heartbeats from
running services indicating that they are still running.

A monitor request contains

* The namespace of the service
* The service name and instance identifier
* Addresses and IDS of all other monitor nodes
* When the next monitor will be sent

Upon receipt and processing, the monitor node immediately sends a
Monitor ACK back to the cluster-service process.

If a monitor gets a heartbeat for a service it's not heard from
before, it creates a new entry in its service table mapping the tuple
of namespace, service and instance id to an instance state structure
and timer.

If a monitor does not hear from the instance again within the time
frame, then the service will enter the unresponsive state. At this
point, the monitor will launch a `service-doctor` process. This
process will attempt to query the main state servers to ascertain
whether the service still ought to be running. If so, then it will
check if this current node is meant to be a monitor. If both checks
are successful, the service doctor process will start sending probe
requests to the cluster-service, hoping to hear back. If nothing is
heard back from that cluster-service process, the service-doctor will
send requests to the other nodes in the monitor set to see if they
have heard from the process. If they have, then this is a network
partition of some kind and the node will enter the PARTITIONED-WAITING
state. Else the node enters the GRACE-PERIOD state. At this point, a
timer is set depending on the state. Once the timer rings, the
service-doctor writes an entry to the master controller setting the
state of the service identifier to FAILED. Then it launches
`cluster-launch --resume <namespace-id>` which will once again
re-examine state and recreate the namespace, including missing
services.

Note that it is possible that something will kill `cluster-launch`
before it completes. To fix this, every namespace has a set of
services that periodically run `cluster-launch --resume
<namespace-id>` anyway.
