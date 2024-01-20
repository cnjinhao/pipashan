#ifndef PIPASHAN_PAXOS_ADAPTER_INCLUDED
#define PIPASHAN_PAXOS_ADAPTER_INCLUDED

#include "../proto.hpp"
#include <functional>
#include <string>

namespace pipashan
{
	/// A adapter defines a group of customized external handlers as a bridge between pipashan node and the consensus object.
	struct adapter_type
	{
		/// Represents a rule according to which the proposals are to be grouped together for accepting.
		std::function<std::string(std::string_view)> grouping_key;

		/// Accepts the request
		std::function<proto::paxos_ack(std::string_view)> accept;
		std::function<proto::paxos_ack(std::string_view)> rollback;

		/// Serializes the current state into a file
		std::function<void(std::filesystem::path)> serialize;

		/// Deserializes the state from a file
		std::function<void(std::filesystem::path)> deserialize;

		bool empty() const noexcept
		{
			return !(grouping_key && accept && rollback && serialize && deserialize);
		}
	};
}
#endif