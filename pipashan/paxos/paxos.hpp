#ifndef PIPASHAN_PAXOS_INCLUDED
#define PIPASHAN_PAXOS_INCLUDED
#include "proposer.hpp"
#include "paxos_core.hpp"
#include <map>
#include <set>

namespace pipashan
{
	class paxos:
		public net::nodeconn::recv_interface
	{
		paxos(const paxos&) = delete;
		paxos& operator=(const paxos&) = delete;
	public:
		using nodecaps_interface = pipashan::abs::nodecaps_interface;
		using nodeconn = net::nodeconn;
		using endpoint = proto::payload::endpoint;

		struct
		{
			std::function<void()> on_ready;
		}signals;

		paxos(const std::string& id, std::size_t working_node_size, nodecaps_interface* nodecaps, nodeconn::pointer conn):
			nodecaps_(nodecaps),
			core_(id, working_node_size, nodecaps),
			acceptor_(id, core_, nodecaps),
			proposer_(id, core_, nodecaps, acceptor_)
		{
			acceptor_.signals.on_ready = [this]{

				_m_deal_delayed_invitations();

				if(signals.on_ready)
					signals.on_ready();
			};

			core_.enable(acceptor_, proposer_);

			if(conn)
			{
				//This node is invited by a remote node
				//core_.insert(conn, paxos_details::node_status::normal, false);
				core_.insert(conn);

				core_.is_paxos_ready(conn, [this](bool ready){});
			}
		}

		~paxos()
		{
			nodecaps_->erase_recv(this);
		}

		paxos_details::paxos_recv_interface* recv_if() noexcept
		{
			return &core_;
		}

		std::map<std::string, proto::payload::paxos_meta> cursors() const
		{
			return acceptor_.cursors();
		}

		bool ready() const noexcept
		{
			return core_.ready();
		}

		void make_ready(bool electing)
		{
			if(core_.ready())
				return;

			core_.state(paxos_details::paxos_core::running_states::ready, electing);

			_m_deal_delayed_invitations();

			if(signals.on_ready)
				signals.on_ready();
		}

		void start(const adapter_type& adapter)
		{
			if(acceptor_.start(adapter, &proposer_))
				nodecaps_->add_recv(this);
		}

		bool insert(nodeconn::pointer conn)
		{
			return (proposer_.insert(conn));
		}

		bool insert(const std::string& node_id)
		{
			if(core_.exists(node_id))
				return true;

			endpoint endpt;
			endpt.id = node_id;

			return proposer_.propose_internal_insert(endpt);
		}

		bool propose(const std::string& data)
		{
			return proposer_.propose(data);
		}

		/// Returns the number of nodes in this paxos instance
		std::size_t size() const noexcept
		{
			return core_.size();
		}

		bool exists(const std::string& node_id) const
		{
			return core_.exists(node_id);
		}

		/// Returns all ndoes that added to this paxos instance except this node.
		std::vector<endpoint> nodes() const
		{
			return core_.nodes();
		}

		/// 
		int notify_ready(const std::string& node)
		{
			return core_.enable_node_ready(node, "");
		}
	private:
		void _m_deal_delayed_invitations()
		{
			std::lock_guard lock{ mutex_ };
			for(auto & node_id : delay_.invitations)
				core_.after_agree(node_id, &proposer_);

			delay_.invitations.clear();
		}

		void _m_invite(nodeconn::pointer conn)
		{
			auto log = nodecaps_->log("paxos-invite");

			//2-Phrase agreement
			//1, the connection has to be added to the acceptor's table if possible.
			//Because the paxos may receive data from the connection
			//2, some part of agreement must be done if the node is ready.
			bool invite = core_.agree(conn, &proposer_);

			if(!invite)
				return;

			if(!core_.ready())
			{
				std::lock_guard lock{ mutex_ };
				//Double-check locking
				if(!core_.ready())
				{
					log.msg("paxos(\"", core_.paxos(), "\") invitation of ndoe(", conn->short_id(), ") delayed");
					delay_.invitations.insert(conn->id());
					return;
				}
			}

			core_.after_agree(conn->id(), &proposer_);
		}
	private:
		// Implements recv_interface.
		virtual void established(nodeconn::pointer conn) override
		{
			nodeconn::recv_interface* core = &core_;
			core->established(conn);
		}

		virtual void online_changed(nodeconn::pointer conn, online_status os) override
		{
			if(online_status::offline != os)
			{
				//There is a new connection.
				_m_invite(conn);
				return;
			}
		}

		virtual recv_status recv(nodeconn::pointer conn, std::shared_ptr<buffer> buf) override
		{
			//The return value of recv provided by proposer is useless, because proposer is extra recv interface.
			return recv_status::success;
		}
	private:
		nodecaps_interface* const nodecaps_;
		paxos_details::paxos_core	core_;
		paxos_details::acceptor	acceptor_;
		paxos_details::proposer	proposer_;

		mutable std::recursive_mutex mutex_;
		struct delayed_tables
		{
			std::set<std::string> invitations;
		}delay_;
	};
}
#endif