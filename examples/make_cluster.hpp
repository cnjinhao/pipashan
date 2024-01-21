
/**
 * This is a simple example to illustrate to create a consensus cluster
 */

#include <pipashan/node.hpp>

namespace make_cluster {

	class consensus_factory : public pipashan::paxos_factory_interface
	{
	private:
		bool make(pipashan::paxos& paxos) noexcept override
		{
			pipashan::adapter_type r;

			//adapter.grouping_key is used for grouping proposals, the proposals in a same group are
			//executed in order. In this example, we give a named "x" group for all proposals. 
			r.grouping_key = [](std::string_view sv) {
				static std::string x{ "x" };
				return x;
				};

			//adapter.accept executes the proposal. It returns paxos_ack::done if the proposalis executed
			//successfully, it returns paxos_ack::failed otherwise.
			r.accept = [this](std::string_view data) {
				if ('+' == data[0]) 
					++value_;
				else
					--value_;

				return pipashan::proto::paxos_ack::done;
			};

			r.rollback = [this](std::string_view data) {
				if ('+' == data[0]) 
					++value_;
				else
					--value_;
				return pipashan::proto::paxos_ack::done;
				};

			//adapter.serialize is used for serializing the current state to a given file
			r.serialize = [this](std::filesystem::path p) {
				std::ofstream os{ p };
				os << value_;
				};

			//adapter.deserialize is used for deserializing the state from a given file, with the method,
			//the consensus instance can restore the states when fatal error occurs.
			r.deserialize = [this](std::filesystem::path p) {
				std::ifstream is{ p };
				is >> value_;
			};

			//Assign the adapter and start the consensus instance
			paxos.start(r);
			return true;
		}


		void destroy() noexcept override
		{
			//The consensus instance is destorying, there is not resurce we should explicitly
			//delete in this example
		}
	private:
		int value_{ 0 }; //This object is a consensus object, the value of the object on consensus nodes are consistent。
	};

	bool run() try
	{
		//Create a node instance to listen 1000 port to make this node accept connections for other nodes.
		pipashan::node node{ "first-node", 	//Identity of this node
							{},				//No node address is specified
							1000,			//listening port
							"./first-node"	//Working path for this node
		};

		node.paxos_factory("demo", std::make_shared<consensus_factory>());

		//The node is configured, now start the service
		node.start();

		//Create a consensus instance.
		auto demo = node.create_paxos("demo", 3);

		//Insert a node whose identity is "second-node" for consensus.
		demo->insert("second-node");
		demo->insert("third-node");

		//Propose to increase the value.
		//if(!demo->propose("+xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
		//	std::cout<<"First proposal is failed"<<std::endl;

		//std::this_thread::sleep_for(std::chrono::seconds{ 10 });

		//Specified the first-node's address.
		const pipashan::node::address addr{ "127.0.0.1", "", 1000 };

		/////////////////////////////////////////////////////////
		//Let's start the second node,
		/////////////////////////////////////////////////////////

		//Create a node instance with the specified identity "second-node"
		pipashan::node node2{ "second-node", 	//Identity of this node
							{addr},			//Specifies the first-node's address,
							0,				//listening port
							"./second-node"	//Working path for this node
		};

		//The demo factory is same as the first node's
		node2.paxos_factory("demo", std::make_shared<consensus_factory>());

		//The node is configured, now start the service.
		node2.start();


		//node3
		//Create a node instance with the specified identity "second-node"
		pipashan::node node3{ "third-node", 	//Identity of this node
							{addr},			//Specifies the first-node's address,
							0,				//listening port
							"./third-node"	//Working path for this node
		};

		//The demo factory is same as the first node's
		node3.paxos_factory("demo", std::make_shared<consensus_factory>());

		//The node is configured, now start the service.
		node3.start();

		//Wait for consensus instance demo on node2 to be ready
		auto d2 = node2.paxos_for("demo", std::chrono::seconds{ 5 });
		while (d2 && !d2->ready())
			std::this_thread::sleep_for(std::chrono::seconds{ 1 });


		//Wait for consensus instance demo on node2 to be ready
		auto d3 = node3.paxos_for("demo", std::chrono::seconds{ 5 });
		while (d3 && !d3->ready())
			std::this_thread::sleep_for(std::chrono::seconds{ 1 });

		demo->propose("+");

		return true;
	} catch(...) {
		return false;
	}
}