#ifndef PIPASHAN_PAXOS_CORE_INCLUDED
#define PIPASHAN_PAXOS_CORE_INCLUDED

#include "../net/server.hpp"
#include <json.hpp>

#include <set>
#include <map>
#include <mutex>
#include <string>

namespace pipashan::paxos_details
{
	/// Node status
	/**
	 * State-machine of a insertion of a node.
	 * 1, When node(A) agrees node(B), it marks the node(B) as not_registered.
	 * 2, node(A) proposes an paxos_internal_insert for registeration the node(B), and marks the node(B) as registered.
	 * 3, If registeration is failed, goto step 2.
	 * 4, If registeration is successful, it changes the status of node(B) as preparing.
	 */
	enum class node_status: std::uint8_t
	{
		normal,
		offline,
		offline_registered,	///< Indicates the node is offline and it has been registered in the paxos,
		registered,			///< Indicates the node has been registered in the paxos, but it hasn't started paxos instance.
		pending
	};

	inline const char* to_string(node_status s)
	{
		const char* texts[] = {"normal", "offline", "offline_registered", "registered", "pending"};

		if(static_cast<std::size_t>(s) < 5)
			return texts[static_cast<std::size_t>(s)];

		return "invalid status";
	}

	enum class agree_results
	{
		disagreed,	///< The remote node is refused
		agreed,		///< The remote node is already the paxos node
		agreed_join	///< The remote node is a new paxos ndoe and the paxos instance on the remote node is not running.
	};


	class paxos_recv_interface
	{
	public:
		virtual ~paxos_recv_interface() = default;
		virtual void recv_paxos_pause(bool paused, const std::string& proposer) = 0;
	};

	class paxos_core:
		public net::nodeconn::recv_interface,
		public paxos_recv_interface
	{
		struct rollbackable
		{
			std::string group_key;
			identifier key;
			std::string proposal;
		};
	public:
		struct remote_node
		{
			proto::payload::endpoint endpoint;
			std::set<net::nodeconn::pointer> conns;

			node_status status{ node_status::offline };
			bool	catch_me_up_delayed{ false };			///< Indicates a remote node's requesting catchup is delayed until this node is ready.
		};
	public:
		using nodeconn = net::nodeconn;
		using endpoint = proto::payload::endpoint;

		enum class running_states
		{
			init,
			pending,
			ready,
		};

		struct stat_type
		{
			std::size_t size;
			std::size_t preparing_nodes;
		};

		enum class use_results
		{
			not_acceptor,
			established,
			used
		};

		paxos_core(const std::string& paxos_id, std::size_t working_node_size, pipashan::abs::nodecaps_interface* nodecaps):
			paxos_(paxos_id),
			nodecaps_(nodecaps),
			working_node_size_(working_node_size)
		{
			if(working_node_size < 3)
				working_node_size_ = 3;
		}

		const std::string& paxos() const noexcept
		{
			return paxos_;
		}

		/// Set the associated proposer. When a conection is removed, nutshell removes the proposer from connection too.
		void enable(nodeconn::paxos_acceptor_interface& acceptor, nodeconn::paxos_proposer_interface& proposer)
		{
			acceptor_ = &acceptor;
			proposer_ = &proposer;
		}

		bool empty() const
		{
			std::lock_guard lock{ mutex_ };
			return acceptors_.empty();
		}

		std::size_t working_node_size() const noexcept
		{
			return working_node_size_;
		}

		void clear()
		{
			std::lock_guard lock{ mutex_ };

			for(auto & m : acceptors_)
			{
				for(auto& conn : m.second.conns)
				{
					_m_erase_recv(conn, false);
				}
			}

			acceptors_.clear();
		}

		/// Uses the specified connection if remote node is an acceptor.
		use_results use(nodeconn::pointer conn)
		{
			std::lock_guard lock{ mutex_ };
			auto i = acceptors_.find(conn->id());
			
			if(i == acceptors_.cend())
				return use_results::not_acceptor;

			bool is_empty = i->second.conns.empty();

			i->second.conns.insert(conn);

			_m_add_recv(conn);

			return (is_empty ? use_results::established : use_results::used);
		}

		nodeconn::pointer random_pick() const
		{
			std::lock_guard lock{ mutex_ };

			for(auto & m : acceptors_)
			{
				if(m.second.conns.empty() && (node_status::normal == m.second.status))
					return *m.second.conns.cbegin();
			}
			return {};
		}

		nodeconn::pointer random_pick_idle(const std::set<std::string>& excepts) const
		{
			std::lock_guard lock{ mutex_ };

			for(auto & m : acceptors_)
			{
				if(m.second.conns.empty())
					continue;
				
				if((node_status::normal != m.second.status) && (0 == excepts.count(m.first)))
					return *m.second.conns.cbegin();
			}
			return {};
		}

		std::optional<node_status> status(const std::string& node_id) const
		{
			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(node_id);

			if(i == acceptors_.cend())
				return {};

			return i->second.status;
		}

		bool agree(nodeconn::pointer conn, abs::proposer_interface* proposer)
		{
			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(conn->id());
			if(i != acceptors_.cend())
			{
				if(i->second.conns.size() == 1)
				{
					i->second.status = node_status::pending;
					
					this->paxos_ready(conn, 0/*query*/, [this, proposer, conn](std::size_t status){
						if(0 == status)
						{
							//the remote node is ready
						}
						else if(1 == status)
						{
							//the remote node is not ready.
							//Just leave it. When it finishes catchup, it tells the cluster it is ready.
						}
						else if(2 == status)
						{
							_m_paxos_create(conn);
						}
						else if(3 == status)
						{
							//error
						}
						else if(4 == status)
						{
							//network error
						}
					});
				}

				return false;
			}

			//The acceptors_.size() doesn't include me, so the number of acceptors plus 1 is the number
			//of paxos nodes in the cluster.
			if(acceptors_.size() + 1 >= working_node_size_)
				return false;

			// Agree the requester node as a paxos node.
			auto & node = _m_insert(conn->id());
			node.conns.insert(conn);
			node.status = node_status::registered;
			node.endpoint = conn->endpt();
			_m_add_recv(conn);

			return true;
		}

		nodeconn::pointer find(const std::string& node_id) const
		{
			std::lock_guard lock{ mutex_ };
			auto i = acceptors_.find(node_id);
			if (i == acceptors_.cend() || i->second.conns.empty())
				return nullptr;

			return *i->second.conns.cbegin();
		}

		void after_agree(const std::string& node_id, abs::proposer_interface* proposer)
		{
			std::lock_guard lock{ mutex_ };
			auto i = acceptors_.find(node_id);
			if(i == acceptors_.cend())
				return;

			if(i->second.conns.empty())
				return;

			_m_after_agree(*i->second.conns.cbegin(), proposer);
		}

		void insert(nodeconn::pointer conn)
		{
			std::lock_guard lock{ mutex_ };
			auto & acc = _m_insert(conn->id());

			acc.endpoint = conn->endpt();

			acc.conns.insert(conn);
			_m_add_recv(conn);
		}

		void insert(nodeconn::pointer conn, node_status s)
		{
			std::lock_guard lock{ mutex_ };
			auto & acc = _m_insert(conn->id());

			if(0 == acc.conns.count(conn))
				acc.status = s;

			acc.endpoint = conn->endpt();

			acc.conns.insert(conn);
			_m_add_recv(conn);
		}

		void erase(nodeconn::pointer conn, bool forced)
		{
			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(conn->id());
			if(i == acceptors_.cend())
				return;

			i->second.conns.erase(conn);
			_m_erase_recv(conn, forced);

			if(i->second.conns.empty())
				i->second.status = node_status::offline;

			//When a connection is broken, it cleans the paxos_pause of the remote node which
			//has not any active connection.
			std::lock_guard lockx{ paxos_pause_.mutex };
			auto u = paxos_pause_.ref_counter.find(conn->id());
			if(u != paxos_pause_.ref_counter.cend())
			{
				//Check if there is not another active connection to the remote node
				if(i->second.conns.empty())
				{
					paxos_pause_.ref_counter.erase(u);
					if(paxos_pause_.ref_counter.empty())
						paxos_pause_.condvar.notify_all();
				}
			}
		}

		/// Enables a specified remote node as paxos node
		/**
		 * @param endpt The endpoint of the remote node which is enabled as paxos node
		*/
		void enable_paxos(const proto::payload::endpoint& endpt)
		{
			// When the node runs the catchup data, it may receive the insert(enable_paxos) itself.
			if(endpt.id == nodecaps_->identity())
				//Returns to avoiding insert myself.
				return;

			auto log = nodecaps_->log("paxos_core-enable_paxos");
			log.msg("enable node(",short_id(endpt.id),") for paxos(\"",paxos_,"\")");

			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(endpt.id);
			if(i == acceptors_.cend())
			{
				auto & acc = _m_insert(endpt.id);

				//Gets the connection for the acceptor.
				auto nodes = nodecaps_->find(endpt.id);

				if(!nodes.empty())
				{
					//The connection is already established
					acc.endpoint = (*nodes.cbegin())->endpt();
					acc.conns = nodes;
					acc.status = node_status::pending;
				}
				else
				{
					//No connection is established but registered
					acc.endpoint = endpt;
					acc.status = node_status::offline_registered;
				}
			}
		}

		template<typename CompleteFunction>
		void paxos_cursors(nodeconn::pointer conn, CompleteFunction complete_fn)
		{
			proto::payload::paxos_cursors paxcur;
			paxcur.paxos = paxos_;

			conn->send(this, paxcur, [this, complete_fn](std::shared_ptr<buffer> buf, std::error_code err){
				std::optional<std::map<std::string, proto::payload::paxos_meta>> cursors;
				if(!err && proto::api::paxos_cursors_resp == buf->pkt()->api_key)
				{
					proto::payload::paxos_cursors_resp resp;
					if(buf->deserialize(resp))
						cursors = std::move(resp.cursors);
				}

				complete_fn(cursors);
			});
		}

		/// Query if a node is ready
		template<typename DoneFunction>
		bool is_paxos_ready(nodeconn::pointer conn, DoneFunction done_fn)
		{
			//Checks if the node of the specified conn is the paxos node
			std::lock_guard lock{ mutex_ };

			auto res = this->use(conn);

			if(use_results::not_acceptor == res)
				return false;

			if(use_results::used == res)
			{
				auto & node = _m_insert(conn->id());

				if (node.status == node_status::normal)
				{
					done_fn(true);
					return true;
				}
			}

			//Query the status from remote node if it is not normal in local.

			//The connection is just added to acceptor list by use(). 
			this->paxos_ready(conn, 0/*query*/, [this, conn, done_fn](std::size_t status){
				//It is ready if status is 0
				done_fn(0 == status);
			});

			return true;
		}

		int enable_node_ready(const std::string& node_id, const std::string& msg)
		{
			int retval = 0;
			auto me = nodecaps_->identity();
			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(node_id);
			if(i != acceptors_.cend())
			{
				if(i->second.conns.empty())
					retval = 1;
				else
					i->second.status = node_status::normal;
			}
			else
				retval = 2;

			std::stringstream ss;
			ss<<"enable_paxos_node("<<node_id<<") = "<<retval;
			if(!msg.empty())
				ss<<" MSG="<<msg;

			this->show_node_list(ss.str());

			return retval;
		}

		std::vector<proto::payload::endpoint> idle_nodes() const
		{
			std::vector<proto::payload::endpoint> endpts;

			std::lock_guard lock{ mutex_ };

			for(auto & acc : acceptors_)
			{
				if(!acc.second.conns.empty())
				{
					if(acc.second.status != node_status::normal)
						endpts.push_back(acc.second.endpoint);
				}
			}
			return endpts;
		}

		std::vector<nodeconn::pointer> connected_nodes() const
		{
			std::vector<nodeconn::pointer> conns;

			std::lock_guard lock{ mutex_ };
			for(auto & acc : acceptors_)
			{
				if(acc.second.conns.empty())
					continue;

				conns.push_back(*acc.second.conns.cbegin());
			}

			return conns;
		}
		

		std::vector<proto::payload::endpoint> inactive_nodes(bool include_unnormal) const
		{
			std::vector<proto::payload::endpoint> endpts;

			std::lock_guard lock{ mutex_ };

			for(auto& acc : acceptors_)
			{
				if(acc.second.conns.empty())
					endpts.push_back(acc.second.endpoint);
				else if(include_unnormal && (acc.second.status != node_status::normal))
					endpts.push_back(acc.second.endpoint);
			}
			return endpts;
		}

		stat_type stat() const
		{
			stat_type s;
			s.preparing_nodes = 0;

			std::lock_guard lock{ mutex_ };

			s.size = acceptors_.size();

			for (auto& m : acceptors_)
			{
				if (node_status::pending == m.second.status || node_status::registered == m.second.status)
					++s.preparing_nodes;
			}
			return s;
		}

		std::vector<nodeconn::pointer> active_nodes(stat_type& s) const
		{
			s.preparing_nodes = 0;

			std::vector<nodeconn::pointer> nodes;
			std::lock_guard lock{ mutex_ };

			s.size = acceptors_.size();
			nodes.reserve(acceptors_.size());

			for(auto & m : acceptors_)
			{
				if(m.second.conns.empty())
				{
					if(node_status::offline_registered == m.second.status)
						++s.preparing_nodes;
				}
				else
				{
					if (node_status::normal == m.second.status)
						nodes.push_back(*m.second.conns.cbegin());
					else if (node_status::pending == m.second.status || node_status::registered == m.second.status)
						++s.preparing_nodes;
				}
			}

			return nodes;
		}

		void show_node_list(const std::string& prefix)
		{
			auto log = nodecaps_->log(paxos_, "show-node_list");

			std::size_t idx = 0;
			std::lock_guard lock{ mutex_ };

			if(acceptors_.empty())
			{
				log.msg(prefix, ": paxos \"", paxos_, "\" has not other nodes");
				return;
			}

			std::stringstream ss;
			ss << prefix << ": paxos(\"" << paxos_ << "\") nodes: [\n";
			for (auto& m : acceptors_)
			{
				ss << "    " << (idx++) << " node(" << short_id(m.first) << ") status:" << to_string(m.second.status) << "  " << m.second.conns.size() << " connections\n";
			}
			ss << "]";

			log.msg(ss.str());
		}

		bool paxos_paused() const
		{
			std::unique_lock lock{ paxos_pause_.mutex };

			return !paxos_pause_.ref_counter.empty();
		}

		void wait_for_paxos_pause()
		{
			std::unique_lock lock{ paxos_pause_.mutex };

			if(paxos_pause_.ref_counter.empty())
				return;

			paxos_pause_.condvar.wait(lock);			
		}

		void serialize_state(const std::filesystem::path& p)
		{
			::nlohmann::json nodes;
			
			std::lock_guard lock{ mutex_ };
			for(auto & acc : acceptors_)
			{
				::nlohmann::json node;

				if(!acc.second.endpoint.addr.ipv4.empty())
					node["ipv4"] = acc.second.endpoint.addr.ipv4;

				if(!acc.second.endpoint.addr.ipv6.empty())
					node["ipv6"] = acc.second.endpoint.addr.ipv6;

				node["port"] = acc.second.endpoint.addr.port;

				node["status"] = static_cast<std::size_t>(acc.second.status);

				nodes[acc.first] = node;
			}

			//Add me
			auto endpt = nodecaps_->endpoint();
			::nlohmann::json me;
			me["port"] = endpt.addr.port;
			if(!endpt.addr.ipv4.empty())
				me["ipv4"] = endpt.addr.ipv4;
			if(!endpt.addr.ipv6.empty())
				me["ipv6"] = endpt.addr.ipv6;

			//me["status"] = static_cast<std::size_t>(status_ready_ ? node_status::normal : node_status::pending);
			me["status"] = static_cast<std::size_t>((running_states::ready == state_) ? node_status::normal : node_status::pending);

			nodes[nodecaps_->identity()] = me;

			

			::nlohmann::json json;

			json["working_node_size"] = working_node_size_;
			json["acceptors"] = nodes;
			std::ofstream os{p};
			os<<json;
		}

		void deserialize_state(const std::filesystem::path& p)
		{
			std::ifstream is{p};
			if(!is)
				return;

			::nlohmann::json json;
			is>>json;

			std::lock_guard lock{ mutex_ };

			if(json.count("working_node_size"))
				working_node_size_ = json.value<std::size_t>("working_node_size", 0);

			for(auto & m : json["acceptors"].items())
			{
				//Skip me, don't add me in acceptors_ table
				if(m.key() == nodecaps_->identity())
					continue;

				auto & node = _m_insert(m.key());

				node.endpoint.id = m.key();

				if(m.value().count("ipv4"))
					node.endpoint.addr.ipv4 = m.value().value<std::string>("ipv4", {});

				if(m.value().count("ipv6"))
					node.endpoint.addr.ipv6 = m.value().value<std::string>("ipv6", {});

				if(m.value().count("port"))
					node.endpoint.addr.port = m.value().value<std::uint16_t>("port", 0);

				if(m.value().count("status") && !node.conns.empty())
					node.status = static_cast<node_status>(m.value().value<std::size_t>("status", 0));
			}
		}

		std::size_t size() const
		{
			std::lock_guard lock{ mutex_ };

			return acceptors_.size();
		}

		std::vector<endpoint> nodes() const
		{
			std::vector<endpoint> endpts;

			std::lock_guard lock{ mutex_ };
			for(auto & acc : acceptors_)
			{
				endpts.emplace_back(acc.second.endpoint);
			}

			return endpts;
		}

		bool exists(const std::string& node_id) const
		{
			std::lock_guard lock{ mutex_ };
			return (acceptors_.count(node_id) != 0);
		}

		void catchup_delay(const std::string& node_id)
		{
			std::lock_guard lock{ mutex_ };
			auto i = acceptors_.find(node_id);
			if(i != acceptors_.cend())
				i->second.catch_me_up_delayed = true;
		}

		std::string rollback(const identifier& key)
		{
			std::string data;

			std::lock_guard lock{ mutex_ };

			auto i = rollback_.keys.find(key);
			if(i != rollback_.keys.cend())
			{
				data.swap(i->second->proposal);
				rollback_.groups.erase(i->second->group_key);
				rollback_.keys.erase(i);
			}
			return data;
		}

		void register_rollback(const std::string& group_key, const identifier& key, std::string proposal)
		{
			std::lock_guard lock{ mutex_ };

			//Remove the existing one before registering a new proposal.
			auto i = rollback_.groups.find(group_key);
			if(i != rollback_.groups.cend())
			{
				rollback_.keys.erase(i->second->key);

				i->second->key = key;
				i->second->proposal = std::move(proposal);

				rollback_.keys[key] = i->second;
			}
			else
			{
				auto rb = std::make_shared<rollbackable>();
				rb->group_key = group_key;
				rb->key = key;
				rb->proposal = std::move(proposal);

				rollback_.groups[group_key] = rb;
				rollback_.keys[key] = rb;
			}
		}

		bool ready() const noexcept
		{
			//return status_ready_;
			return (running_states::ready == state_);
		}

		void state(running_states s, bool elected)
		{	
			if(running_states::ready == s && elected)
			{
				//This node is ready now by electing
				state_ = s;

				auto idles = this->idle_nodes();

				auto log = nodecaps_->log(paxos_, "elect");
				
				for(auto & endpt : idles)
				{
					auto conns = nodecaps_->find(endpt.id);
					if(conns.empty())
						continue;

					log.msg("send invite-catchup to node(", (*conns.cbegin())->id(), ")");

					this->paxos_ask_catchup(*conns.cbegin());
				}

				acceptor_->delay_catchup();
			}

			state_ = s;
		}

		running_states state() const
		{
			return state_;
		}

		/// Asks a remote node to perform catchup
		bool paxos_ask_catchup(nodeconn::pointer conn)
		{
			proto::payload::paxos_catchup_accepted paxacc;
			paxacc.paxos = paxos_;
			return conn->send(paxacc);
		}

		/// Reads the new endpoints of nodes from a ready node.
		template<typename DoneFunction>
		void paxos_endpoint(DoneFunction done_fn)
		{
			auto me = nodecaps_->identity();
			proto::payload::paxos_endpoint paxept;
			paxept.paxos = paxos_;

			auto conn = this->random_pick();

			if(!conn)
			{
				conn = this->random_pick_idle(std::set<std::string>{});
				if(!conn)
				{
					done_fn(false);
					return;
				}
			}
			
			conn->send(this, paxept, [this, done_fn](std::shared_ptr<buffer> buf, std::error_code err){

				bool status = false;
				if((!err) && proto::api::paxos_endpoint_resp == buf->pkt()->api_key)
				{
					proto::payload::paxos_endpoint_resp resp;
					if(buf->deserialize(resp))
					{
						status = true;
						std::lock_guard lock{ mutex_ };

						for(auto & endpt : resp.nodes)
						{
							auto i = acceptors_.find(endpt.id);
							if(i == acceptors_.cend() || !i->second.conns.empty())
								continue;

							i->second.endpoint = endpt;
						}
					}
				}

				done_fn(status);
			});
		}

		/// Queries a node ready status or changes the status to be ready.
		/**
		 * @param conn The connection to the node.
		 * @param act Action. 0=Query; 1=Make it ready; 2: Make it ready by electing.
		 * @param done_fn A function of void(bool).
		 * @return true if the paxos_ready is sent, false otherwise.
		 */
		template<typename DoneFunction>
		bool paxos_ready(nodeconn::pointer conn, std::uint8_t act, DoneFunction done_fn)
		{
			//Query the status of the remote node;
			proto::payload::paxos_ready paxrdy;
			paxrdy.paxos = paxos_;
			paxrdy.action = act;

			conn->send(this, paxrdy, [this, conn, done_fn](std::shared_ptr<buffer> buf, std::error_code err){
				auto log = nodecaps_->log(paxos_, "paxos_ready");

				if (!err && (proto::api::ack == buf->pkt()->api_key))
				{
					if (buf->pkt()->len == 0)
					{
						log.msg("node(", conn->short_id(), ") is ready");
						//ready
						this->enable_node_ready(conn->id(), "paxos_ready");
					}
					else
						log.msg("node(", conn->short_id(), ") is not ready");

					done_fn(buf->pkt()->len);
				}
				else
				{
					log.err("failed to paxos_ready of node(", conn->short_id(), ")");
					done_fn(4);
				}

			});
			return true;
		}

		template<typename DoneFunction>
		bool paxos_notify_ready(const std::string& node, DoneFunction done_fn)
		{
			auto conns = this->connected_nodes();
			if(conns.empty())
				return false;

			std::size_t size = 0;

			for(auto & conn : conns)
			{
				if(conn->id() == node)
					continue;

				++size;
			}

			proto::payload::paxos_notify_ready paxntf;
			paxntf.paxos = paxos_;
			paxntf.node = node;

			auto counter = std::make_shared<std::atomic<std::size_t>>(size);

			for(auto & conn : conns)
			{
				if(conn->id() == node)
					continue;
				
				conn->send(this, paxntf, [this, done_fn, counter](std::shared_ptr<buffer> buf, std::error_code err){
					
					if(!err && proto::api::paxos_notify_ready == buf->pkt()->api_key)
					{
					}

					if(0 == --(*counter))
						done_fn();
				});

			}

			return true;
		}

		bool pause(bool paused)
		{
			return _m_pause(paused, true);
		}
	private:
		// Implements the paxos_recv_interface
		virtual void recv_paxos_pause(bool paused, const std::string& proposer) override
		{
			std::lock_guard lock{ paxos_pause_.mutex };
			if(false == paused)
			{
				auto i = paxos_pause_.ref_counter.find(proposer);
				if(i != paxos_pause_.ref_counter.cend())
				{
					if(i->second > 1)
					{
						--(i->second);
						return;
					}

					paxos_pause_.ref_counter.erase(i);

					//Notify the suspended threads to continue preparing.
					if(paxos_pause_.ref_counter.empty())
						paxos_pause_.condvar.notify_all();
				}
			}
			else
			{
				paxos_pause_.ref_counter[proposer] += 1;
			}
		}
	private:
		bool _m_pause(bool paused, bool rollback)
		{
			this->recv_paxos_pause(paused, nodecaps_->identity());

			proto::payload::paxos_pause paxpas;
			paxpas.paxos = paxos_;
			paxpas.paused = paused;

			std::atomic<std::size_t> count = 0;

			auto nodes = connected_nodes();

			std::size_t total = nodes.size();

			std::vector<nodeconn::pointer> drawbacks;

			std::mutex mutex;
			std::condition_variable condvar;

			for(auto & conn: nodes)
			{
				conn->send(this, paxpas, [this, &count, total, conn, &drawbacks, &mutex, &condvar](std::shared_ptr<buffer> buf, std::error_code err){
					if(!err)
					{
						std::lock_guard lock{ mutex };
						drawbacks.push_back(conn);
					}

					if(total == ++count)
					{
						std::lock_guard lock{ mutex };
						condvar.notify_one();
					}

				});
			}

			std::cv_status s = std::cv_status::no_timeout;

			{
				std::unique_lock lock{ mutex };
				s = condvar.wait_for(lock, std::chrono::seconds{15});
			}

			if((std::cv_status::timeout == s) || (total != count))
			{
				if(rollback)
					_m_pause(!paused, false);
				return false;
			}

			return true;
		}

		void _m_paxos_create(nodeconn::pointer conn)
		{
			_m_paxos_create(conn, [this, conn](bool success) mutable {
				if(success)
					return;

				std::this_thread::sleep_for(std::chrono::milliseconds{500});

				conn = _m_next(conn);
				if(conn)
					_m_paxos_create(conn);
				else
					return;
			});

		}



		template<typename DoneFunction>
		void _m_paxos_create(nodeconn::pointer conn, DoneFunction done_fn)
		{
			auto log = nodecaps_->log("paxos-create");

			log.msg("send paxos_create(\"", paxos_, "\") to the node(\"", conn->short_id(), "\")");
			proto::payload::paxos_create paxcrt;
			paxcrt.paxos = paxos_;
			paxcrt.working_node_size = static_cast<std::uint8_t>(working_node_size());

			conn->send(this, paxcrt, [this, done_fn](std::shared_ptr<buffer> buf, const std::error_code& err) mutable {
				if((!err) && (proto::api::ack == buf->pkt()->api_key))
				{
					done_fn(true);
				}
				else
					done_fn(false);
			});
		}

		nodeconn::pointer _m_next(nodeconn::pointer conn)
		{
			if(!conn)
				return nullptr;

			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(conn->id());
			if(i == acceptors_.cend())
				return nullptr;

			if(i->second.conns.empty())
				return nullptr;

			for(auto u = i->second.conns.cbegin(); u != i->second.conns.cend(); ++u)
			{
				if(*u == conn)
				{
					++u;
					if(u == i->second.conns.cend())
						return *i->second.conns.cbegin();

					return *u;
				}
			}

			return *i->second.conns.cbegin();
		}

		void _m_after_agree(nodeconn::pointer conn, abs::proposer_interface* proposer)
		{
			nodecaps_->exec().run([this, conn, proposer]() mutable {
				
				if(proposer->propose_internal_insert(conn->endpt()))
				{
					_m_paxos_create(conn, [this, conn, proposer](bool success) mutable {
						if(success)
							return;

						std::this_thread::sleep_for(std::chrono::milliseconds{ 500 });
						conn = _m_next(conn);
						if(conn)
							_m_after_agree(conn, proposer);
					});

					return;
				}

				std::this_thread::sleep_for(std::chrono::milliseconds{ 500 });

				conn = _m_next(conn);
				if(conn)
					_m_after_agree(conn, proposer);
			});
		}

		void _m_add_recv(nodeconn::pointer& p)
		{
			p->add(this);
			p->add_paxos(paxos_, acceptor_, proposer_);
		}

		void _m_erase_recv(nodeconn::pointer p, bool forced)
		{
			p->erase(this, forced);
			p->erase_paxos(paxos_, forced);
		}

		remote_node& _m_insert(const std::string& id)
		{
			return acceptors_[id];
		}

	private:
		// Overrides the recv_interface

		/// Attaches the paxos instance if the remote is an acceptor of the paxos.
		/// This method is invoked by manager, unlike online_changed() and recv() which are invoked by the nodeconn
		virtual void established(nodeconn::pointer conn) override
		{
			/// Attaches the paxos instance if the remote is an acceptor of the paxos.
			std::lock_guard lock{ mutex_ };

			auto i = acceptors_.find(conn->id());
			if (i == acceptors_.cend())
				return;

			i->second.conns.insert(conn);

			if(i->second.endpoint != conn->endpt())
			{
				//The endpoint of remote node has been changed, E.g. notify the connected nodes.
				i->second.endpoint = conn->endpt();
			}

			_m_add_recv(conn);
		}

		virtual void online_changed(nodeconn::pointer conn, online_status os) override
		{
			if(online_status::offline != os)
				return;

			//It is safe to erase the connection forcefully here,
			//The paxos_core object is alive when detachs it from the nodeconn.
			this->erase(conn, true);
		}

		virtual recv_status recv(nodeconn::pointer, std::shared_ptr<buffer>) override
		{
			return recv_status::success;
		}
	private:
		std::string const paxos_;
		pipashan::abs::nodecaps_interface* const nodecaps_;
		nodeconn::paxos_acceptor_interface* acceptor_{ nullptr };	///< acceptor module
		nodeconn::paxos_proposer_interface* proposer_{ nullptr };
		mutable std::recursive_mutex mutex_;

		//bool status_ready_{ false };	///< Indicates this node whether catches up the distributed status.
		running_states state_{ running_states::init };

		std::size_t working_node_size_;
		std::map<std::string, remote_node> acceptors_;

		struct paxos_pause
		{
			mutable std::mutex mutex;
			std::condition_variable condvar;
			std::map<std::string, std::size_t>  ref_counter;
		}paxos_pause_;

		struct rollback
		{
			std::map<std::string, std::shared_ptr<rollbackable>> groups;
			std::map<identifier, std::shared_ptr<rollbackable>> keys;
		}rollback_;
	};
}

#endif