# Pipashan

Pipashan is a C++20 header-only decentralized consensus library that facilitates the distributed computing.

## Purpose
Features:
* High performance
* Low-resource consumption
* Fault-tolerance
* High availability and scalability

## Dependencies 
* Boost.asio
* [JSON for Modern C++](https://github.com/nlohmann/json)

## Examples

There are some examples to show you the idea how to use Pipashan.

### Create A Simple Cluster

This example shows the basic concepts of Pipashan. For simplicity's sake, we create 2 nodes in a single process to illustrate the way to build a cluster.


```C++
//This is Demo 1
#include <pipashan/node.hpp>
#include <iostream>

class demo_factory: public pipashan::paxos_factory_interface
{
private:
	bool make(pipashan::paxos* paxos) noexcept override
	{
		pipashan::paxos::reactor_type r;

		r.table_key = [](std::string_view){
			return "x";
		};

		r.accept = [this](std::string_view sv){
			std::cout<<sv<<std::endl;
			
			return pipashan::proto::paxos_ack::done;
		};

		r.rollback = [](const pipashan::proto::payload::paxos_rollback&){
			return pipashan::proto::paxos_ack::done;
		};

		r.serialize = [this](std::filesystem::path p){
			//Empty
		};

		r.deserialize = [this](std::filesystem::path p){
			//Empty
		};

		paxos->start(r);
		return true;
	}

	void destroy() noexcept override
	{

	}
};

int main()
{
	//Create a node instance to listen 1000 port to make this node accept connections for other nodes.
	pipashan::node node{"first-node", 	//Identity of this node
						{},				//No node address is specified
						1000,			//listening port
						"./first-node"	//Working path for this node
						};

	node.paxos_factory("demo", std::make_shared<demo_factory>());

	//The node is configured, now start the service
	node.start();

	//Create a paxos instance
	auto demo = node.create_paxos("demo", 3);

	//Insert a node whose identity is "second-node" for consensus.
	demo->insert("second-node");

	//Propose a text
	demo->propose("Hello, Pipashan!");


	/////////////////////////////////////////////////////////
	//Let's start the second node
	/////////////////////////////////////////////////////////

	//Specified the first-node's address.
	pipashan::node::address addr{"127.0.0.1", "", 1000};

	//Create a node instance with the specified identity "second-node"
	pipashan::node node2{"second-node", 	//Identity of this node
						{addr},			//Specifies the first-node's address,
						0,				//listening port
						"./second-node"	//Working path for this node
						};

	//The demo factory is same as the first node's
	node2.paxos_factory("demo", std::make_shared<demo_factory>());

	//The node is configured, now start the service.
	node2.start();

	//Exit the program in 1 hour
	std::this_thread::sleep_for(std::chrono::hours{1});	
}
```

Before starting the second node, register the `second-node` to the paxos `demo` on the first node. After propose a text, the text `"Hello, Pipashan!"` will be shown on the screen. 

When the second node connects to the first node successfully, the first-node will invite the second-node to paxos `demo` and the second node will synchronize the state from the first node. Then you will see another `"Hello, Pipashan!"` on the screen.


### Serialization of States

In last `Demo 1`, we just gave empty serializa() and empty deserialize() method for the reactor, it implies the state of the cluster is not changed.

When a node proposes a data successfully, it put the data into a task-queue which is stored in a local file. When the task-queue reaches a specific length, the node will serialize the current states to a snapshot file by using reactor.serialize() and clear the task-queue. The benefit of the procedure is to reduce the local storage usage and the complexity of the synchronization.

Let's start a `Demo 2` to illustrate how to serialize the state of the cluster.

```C++
//Demo 2
#include <pipashan/node.hpp>
#include <fstream>

class demo_factory: public pipashan::paxos_factory_interface
{
private:
	bool make(pipashan::paxos* paxos) noexcept override
	{
		pipashan::paxos::reactor_type r;

		r.table_key = [](std::string_view){
			return "x";
		};

		r.accept = [this](std::string_view sv){
			
			std::lock_guard lock{ mutex_ };
			if('+' == sv[0])
				++value_;
			else
				--value_;
			
			return pipashan::proto::paxos_ack::done;
		};

		r.rollback = [](const pipashan::proto::payload::paxos_rollback&){
			return pipashan::proto::paxos_ack::done;
		};

		r.serialize = [this](std::filesystem::path p){
			std::lock_guard lock{ mutex_ };

			std::ofstream os{p};
			os<<value_;
		};

		r.deserialize = [this](std::filesystem::path p){
			std::lock_guard lock{ mutex_ };

			std::ifstream is{p};
			is>>value_;
		};

		paxos->start(r);
		return true;
	}

	void destroy() noexcept override
	{
	}
private:
	mutable std::recursive_mutex mutex_;
	int value_{ 0 };
};

int main()
{
	//Create a node instance to listen 1000 port to make this node accept connections for other nodes.
	pipashan::node node{"first-node", 	//Identity of this node
						{},				//No node address is specified
						1000,			//listening port
						"./first-node"	//Working path for this node
						};

	node.paxos_factory("demo", std::make_shared<demo_factory>());

	//The node is configured, now start the service
	node.start();

	//Create a paxos instance
	auto demo = node.create_paxos("demo", 3);

	//Insert a node whose identity is "second-node" for consensus.
	demo->insert("second-node");

	//Propose to increase the value.
	demo->propose("+");

	/////////////////////////////////////////////////////////
	//Let's start the second node
	/////////////////////////////////////////////////////////

	//Specified the first-node's address.
	pipashan::node::address addr{"127.0.0.1", "", 1000};

	//Create a node instance with the specified identity "second-node"
	pipashan::node node2{"second-node", 	//Identity of this node
						{addr},			//Specifies the first-node's address,
						0,				//listening port
						"./second-node"	//Working path for this node
						};

	//The demo factory is same as the first node's
	node2.paxos_factory("demo", std::make_shared<demo_factory>());

	//The node is configured, now start the service.
	node2.start();

	//Exit the program in 1 hour
	std::this_thread::sleep_for(std::chrono::hours{1});	
}
```
