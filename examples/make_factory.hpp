/**
 * This is a simple example to show the way to create a consensus adapter and a pipashan node.
 */

#include <pipashan/node.hpp>

namespace make_factory{

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
		pipashan::node node{ "demo-node", 		//Identity of this node
							{},				//No node address is specified
							1000,			//listening port
							"./demo-node"	//Working path for this node
		};

		//When the program starts, it creates a "demo-node" folder in current working path if the folder doesn't exists.

		//Assigns the consensus factory
		node.paxos_factory("demo", std::make_shared<consensus_factory>());

		//The node is configured, now start the service
		node.start();


		//Create a consensus instance.
		//The number of consensus nodes is 3, when other nodes connect to the cluster,
		//there are 2 nodes will be chosen to be the consensus node for consensus "demo"
		auto demo = node.create_paxos("demo", 3);


		return true;
	} catch(...) {
		return false;
	}
}