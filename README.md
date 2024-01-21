# Pipashan

Pipashan is a C++20 header-only decentralized consensus library that facilitates the distributed computing. Pipashan implements a QueuedPaxos which is a Paxos protocol variant that implements effectively automatic conflict resolution.

## Purpose

Features:
* Fault-tolerance
* High performance
* Low-resource consumption
* Effectively automatic conflict resolution
* High availability and scalability

## Getting Started

### Installation

Prepare the dependencies:

* [Boost.asio](https://www.boost.org)
* [JSON for Modern C++](https://github.com/nlohmann/json) `already in 3rdparty`

Copy the folder `pipashan` to the include folder, then use `#include<pipashan/node.hpp>` in your source code.

### Concepts

Consensus is a about the process of coming to a complete agreement across several nodes in a cluster. Pipashan hides the complicated tasks of consensus and provides intuitive methods to implement consensus for distributed computing.

```C++
//cons is a pointer to a consensus instance of a node.
if(cons->propose("Pipashan"))
{
	//Reaching a consensus in the cluster.
	//The nodes which have same consensus instances in the cluster have accepted the text "Pipashan".
}
else
{
	//No nodes have accepted the text "Pipashan".
	//Some nodes maybe not received, others maybe rollback the text.
}
```

The pointer `cons` points to a consensus instance which is maintained by pipashan.

### Consensus Factory

The consensus factory is assigned to the pipashan node for creating the consensus instance. The key task of consensus factory is to define an adapter.

```C++
class consensus_factory : public pipashan::paxos_factory_interface
{
private:
	bool make(pipashan::paxos& paxos) noexcept override
	{
		pipashan::adapter_type adapter;

		//Implement the adapter
		//adapter.grouping_key = ...
		//adapter.accept = ...
		//adapter.rollback = ...
		//adapter.serialize = ...
		//adapter.deserialize = ...

		//Assign the adapter and start the consensus instance
		paxos.start(adapter);
		return true;
	}

	void destroy() noexcept override
	{
		//The consensus instance is destorying, there is no resurce we should explicitly
		//delete in this example
	}
};

//Assigns the factory with a given name "consensus"
node.paxos_factory("consensus", std::make_shared<consensus_factory>());

//Create the consensus instance with the specified name "consensus" and the number of consensus
//nodes is 3.
auto cons = node.create_paxos("consensus", 3);
```
The pipashan node can create multiple consensus instances, but it creates only one consensus instance for a given name.

### Adapter

The adapter is a bridge between the pipashan and your consensus object. For example, let's define an adapter to make a value for consensus in the cluster.

```C++
class consensus_factory : public pipashan::paxos_factory_interface
{
private:
	bool make(pipashan::paxos& paxos) noexcept override
	{
		pipashan::adapter_type adapter;

		//Implement the adapter

		//grouping_key is used for grouping proposals, the proposals in a same group are executed
		//in order. In this example, we give a named "x" group for all proposals.
		//When pipashan receives a unknown grouping_key, it automatically creates the associated
		//data for the key.
		adapter.grouping_key = [](std::string_view) {
			static std::string x{ "x" };
			return x;
		};

		//adapter.accept executes the proposal. It returns paxos_ack::done if the proposal is executed
		//successfully, it returns paxos_ack::failed otherwise.
		adapter.accept = [this](std::string_view data) {
			if ('+' == data[0]) 
				++value_;
			else
				--value_;

			return pipashan::proto::paxos_ack::done;
		};

		adapter.rollback = [](std::string_view data) {
			if('+' == data[0])
				--value_;
			else
				++value_;
			return pipashan::proto::paxos_ack::done;
		};

		//adapter.serialize is used for serializing the current state to a given file
		adapter.serialize = [this](std::filesystem::path p) {
			std::ofstream os{ p };
			os << value_;
		};

		//adapter.deserialize is used for deserializing the state from a given file, with the method,
		//the consensus instance can restore the states when fatal error occurs.
		adapter.deserialize = [this](std::filesystem::path p) {
			std::ifstream is{ p };
			is >> value_;
		};

		//Assign the adapter and start the consensus instance
		paxos.start(adapter);
		return true;
	}

	void destroy() noexcept override
	{
		//The consensus instance is destorying, there is no resurce we should explicitly
		//delete in this example
	}
private:
	std::size_t value_{ 0 }; //The object of consensus.
};
```
