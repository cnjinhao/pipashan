#ifndef PIPASHAN_PAXOS_ABSTRACTION_INCLUDED
#define PIPASHAN_PAXOS_ABSTRACTION_INCLUDED

#include <optional>
#include "../net/server.hpp"

namespace pipashan::paxos_details::abs
{
	//Definitions of abstractions for the module dependencies.

	class preparer_interface
	{
	public:
		virtual ~preparer_interface() = default;
		virtual const std::string& grouping_key() const noexcept = 0;
		virtual void is_first(std::function<void(bool)>) const = 0;
	};

	class proposer_interface
	{
	public:
		using nodeconn = net::nodeconn;

		virtual ~proposer_interface() = default;

		/// Returns the paxos meta of the data.
		virtual proto::payload::paxos_meta make_paxos_meta(bool internal, const std::string& data) = 0;

		virtual bool propose_internal_insert(const proto::payload::endpoint&) = 0;

		virtual void activate_proposal(const identifier& key) = 0;

		/// When the first proposal in queue of proposals is changed, acceptor calls this method to infor the proposer
		virtual void head_proposal_changed(bool internal, const std::string& table_key, const std::optional<identifier>& ) = 0;
	};
}
#endif