Janus is a demo for using [Narwhal](https://github.com/MystenLabs/sui/tree/main/narwhal) DAG-Based consensus algorithm to achieve BFT and serializable operations with CRDTs.
Janus works as a Key-value database cluster with PN-Counter and OR-Set support, you can use Janus client to interact with the any of the server.

## How to run
### Requirements

- .Net SDK 6.0+
- Python 3.10+
- Ubuntu 22.04

### Run locally

```
$cd BFT-CRDT-Client/scripts
$./start_servers.py start [number_of_servers] 
```
number_of_servers must >= 4

### Run remotely
```
$cd BFT-CRDT-Client/scripts
$./start_servers.py rstart [number_pre_servers] [ip1, ip2]')
```

### Connect to a server
```
$cd BFT-CRDT-Client
$dotnet run <mode> (<ip> <port> | <benchmark config file> <oneshot?>
```

Use <mode=1> to run an interactive client - see in client help string for commands.
Use <mode=2> to run benchmark - see `/BFT-CRDT-Client/benchmark_config_example.json` for details.
