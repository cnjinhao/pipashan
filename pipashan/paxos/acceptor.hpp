#ifndef PIPASHAN_PAXOS_ACCEPTOR_INCLUDED
#define PIPASHAN_PAXOS_ACCEPTOR_INCLUDED

#include "adapter.hpp"
#include "../nodecaps.hpp"
#include "../transfer.hpp"
#include "abstraction.hpp"
#include "paxos_core.hpp"
#include "catcher.hpp"

#include <deque>
#include <map>
#include <optional>
#include <mutex>
#include <condition_variable>


namespace pipashan::paxos_details
{
	class acceptor:
		public net::nodeconn::paxos_acceptor_interface
	{
		class queue_invoker: public task_queue::invoker_interface
		{
		public:
			queue_invoker(acceptor* self):
				self_(self)
			{}
		private:
			virtual bool task(const identifier& key, bool internal, std::string_view data) override
			{
				return self_->_m_apply_request(key, internal, data);
			}

			virtual void internal(const std::filesystem::path& p) override
			{
				self_->core_.deserialize_state(p);
			}

			virtual void external(const std::filesystem::path& p) override
			{
				self_->adapter_.deserialize(p);
			}
		private:
			acceptor * const self_;
		};

		class conflicted_request
		{
			struct context
			{
				net::nodeconn::pointer conn;
				std::uint32_t pktcode;
				bool internal;
			};
		public:
			using nodeconn = net::nodeconn;

			conflicted_request(acceptor& self):
				self_(self)
			{

			}

			std::size_t size() const
			{
				return table_.size();
			}

			void insert(nodeconn::pointer conn, std::uint32_t pktcode, const proto::payload::paxos_meta& meta)
			{
				auto & ctx = table_[meta.key];
				ctx.conn = conn;
				ctx.pktcode = pktcode;
				ctx.internal = meta.internal;
			}

			void paxos_propose_resp(const identifier& key, proto::paxos_ack ack)
			{
				auto i = table_.find(key);
				if(i == table_.cend())
					return;

				self_._m_paxos_propose_resp(i->second.conn, i->second.pktcode, key, i->second.internal, ack, key);

				table_.erase(i);
			}

			std::vector<identifier> offline(nodeconn::pointer conn)
			{
				std::vector<identifier> removes;

				for(auto i = table_.cbegin(); i != table_.cend();)
				{
					if(i->second.conn == conn)
					{
						removes.push_back(i->first);

						i = table_.erase(i);
					}
					else
						++i;
				}
				return removes;
			}
		private:
			acceptor& self_;
			std::map<identifier, context> table_;
		};

		class preparer_impl
			: public abs::preparer_interface
		{
		public:
			preparer_impl(acceptor* self, bool internal, const identifier& key):
				self_(self),
				internal_(internal),
				key_(key)
			{
			}

			~preparer_impl()
			{
				std::optional<identifier> key;
				{
					std::lock_guard lock{ self_->mutex_ };
					key = self_->_m_remove_from_queue(internal_, grouping_key_, key_);
				}
				//Activates the proposal to try_notify if the proposal is proposed by this node.
				if(key)
					self_->proposer_->activate_proposal(key.value());
			}

			void grouping_key(const std::string& grp_key)
			{
				grouping_key_ = grp_key;
			}

			virtual const std::string& grouping_key() const noexcept override
			{
				return grouping_key_;
			}

			virtual void is_first(std::function<void(bool)> fn) const override
			{
				std::lock_guard lock{ self_->mutex_ };
				fn(self_->_m_is_first(internal_, grouping_key_, key_));
			}
		private:
			acceptor* const self_;
			bool internal_;
			identifier key_;
			std::string grouping_key_;
		};
	public:
		using nodeconn = net::nodeconn;
		using nodecaps_interface = pipashan::abs::nodecaps_interface;

		struct request_data
		{
			proto::payload::paxos_meta meta;
			std::string group_key;
			std::string body;
			std::time_t timestamp;
		};

		struct
		{
			std::function<void()> on_ready;
		}signals;

		/// Constructs the instance of acceptor
		/**
		 * @param id	Identifier of paxos instance
		 */
		acceptor(const std::string& id, paxos_core& core, nodecaps_interface* nodecaps):
			paxos_(id),
			nodecaps_(nodecaps),
			core_(core),
			invoker_(this),
			taskque_(id, invoker_, nodecaps),
			delay_(*this)
		{
		}

		~acceptor()
		{
			core_.clear();
		}

		std::map<std::string, proto::payload::paxos_meta> cursors() const
		{
			return taskque_.cursors();
		}

		/// Starts the acceptor and applies the local queue
		/**
		 * There are different circumstances when start is called.
		 * 1, Local queue is empty and a remote paxos node is inviting this node to add to the paxos instance.
		 */
		bool start(const adapter_type & ra, abs::proposer_interface* prop)
		{
			auto me = nodecaps_->identity();
			
			auto log = nodecaps_->log("acceptor-start");
			std::lock_guard lock{ mutex_ };
			
			if((!adapter_.empty()) || ra.empty() || !prop)
			{
				if(!adapter_.empty())
				{
					log.err("paxos(\"",paxos_,"\") has been started");
					return false;
				}
				
				if(ra.empty())
				{
					if(!ra.accept)
						log.err("reactor.accept is empty");
					if(!ra.rollback)
						log.err("reactor.rollback is empty");
					if(!ra.serialize)
						log.err("reactor.serialize is empty");
					if(!ra.deserialize)
						log.err("reactor.deserialize is empty");
					if(!ra.grouping_key)
						log.err("reactor.grouping_key is empty");
				}

				if(!prop)
					log.err("proposer is not attached");

				log.err("failed to start paxos(\"", paxos_, "\")");

				return false;
			}

			proposer_ = prop;
			adapter_ = ra;

			//Acceptor is done configuring, it runs the snapshot and the queue
			//for establising the state.
			taskque_.apply();

			//Run the catchup if there is a connection which node status is normal
			auto conn = core_.random_pick();
			if(conn)
			{
				log.msg("This node is invited by paxos(\"", paxos_, "\")");
				//There is a existing connection which node status is normal, it indicates the node 
				//is a inviter who invited this node to be a paxos node.

				//Run the catchup with a certain connection.
				taskque_.reset_write_state();
				_m_catchup(conn, false);
			}
			else if(!core_.empty())
			{
				//The node is resotred from a fatal or restarting up.
				log.msg("This node is restoring from fatal");
				_m_join();
			}
			else
			{
				if(taskque_.empty())
				{
					log.msg("This node created new paxos instance");

					//This node is the first node of the paxos instance.
					this->_m_paxos_insert_me();
				}
				else
					log.msg("This node has not empty local queue");
			}

			log.msg("paxos(\"",paxos_,"\") is started");

			return true;
		}

		std::string grouping_key(const std::string& data) const
		{
			return adapter_.grouping_key(data);
		}
	private:
		void _m_paxos_ack(nodeconn::pointer conn, std::uint16_t pktcode, const identifier& key, bool internal, proto::paxos_ack ack)
		{
			proto::payload::paxos_ack paxack;
			paxack.meta.paxos = paxos_;
			paxack.meta.key = key;
			paxack.meta.internal = internal;
			paxack.ack = ack;
			conn->send(pktcode, paxack);
		}

		void _m_paxos_propose_resp(nodeconn::pointer& conn, std::uint16_t pktcode, const identifier& key, bool internal, proto::paxos_ack ack, const identifier& first_key)
		{
#ifdef SHOWLOG_PROPOSE
			auto log = nodecaps_->log("paxos-propose-resp");
			log.msg("key:", key.short_hex(), " ack = ", to_string(ack), " first-key:", first_key.short_hex());
#endif
			proto::payload::paxos_propose_resp paxrsp;
			paxrsp.meta.paxos = paxos_;
			paxrsp.meta.key = key;
			paxrsp.meta.internal = internal;
			paxrsp.ack = ack;
			paxrsp.first = first_key;

			conn->send(pktcode, paxrsp);
		}
	private:
		proto::paxos_ack _m_paxos_propose(nodeconn::pointer& conn, std::uint32_t pktcode, const proto::payload::paxos_propose& paxpro, identifier& first_key)
		{
			auto time_pt = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

			auto const tmstamp = std::time(nullptr);

			auto & req = dataholder_[paxpro.meta.key];
			req.meta = paxpro.meta;
			req.body = std::move(paxpro.body);
			req.timestamp = tmstamp;

			if(paxpro.meta.internal)
			{
				internal_queue_.push_back(&req);

				first_key = internal_queue_.front()->meta.key;

				if (internal_queue_.size() == 1)
				{
					proposer_->head_proposal_changed(true, std::string{}, req.meta.key);
					return proto::paxos_ack::ready;
				}
			}
			else
			{
				auto group_key = adapter_.grouping_key ? adapter_.grouping_key(paxpro.body) : std::string{};

				if(group_key.empty())
					return proto::paxos_ack::ready;

				auto & queue = table_[group_key];

				queue.push_back(&req);

				first_key = queue.front()->meta.key;

				req.group_key = group_key;
				if (queue.size() == 1)
				{
					proposer_->head_proposal_changed(false, group_key, req.meta.key);
					return proto::paxos_ack::ready;
				}
			}

			delay_.requests.insert(conn, pktcode, paxpro.meta);
			return proto::paxos_ack::conflicted;
		}
	public:
		// Overrides the paxos_acceptor_interface
		virtual void paxos_propose(nodeconn::pointer conn, std::uint32_t pktcode, const proto::payload::paxos_propose& paxpro) override
		{
			identifier first_key;
			std::lock_guard lock{ mutex_ };
			auto ack = _m_paxos_propose(conn, pktcode, paxpro, first_key);

			//The propose_resp doesn't contain the first key in the queue when it is sent by paxos-propose
			_m_paxos_propose_resp(conn, pktcode, paxpro.meta.key, paxpro.meta.internal, ack, first_key);
		}

		virtual void paxos_cancel(nodeconn::pointer, const proto::payload::paxos_cancel& paxcan) override
		{
			std::lock_guard lock{ mutex_ };

			auto i = dataholder_.find(paxcan.meta.key);

			if(i != dataholder_.end())
			{
				_m_remove_from_queue(paxcan.meta.internal, i->second.group_key, paxcan.meta.key);
				dataholder_.erase(i);
			}
		}

		virtual void delay_catchup() override
		{
			std::lock_guard lock{ mutex_ };
			for(auto & m : delay_.catchup)
			{
				for(auto & conn : m.second)
				{
					if(core_.paxos_ask_catchup(conn))
						break;
				}
			}

			delay_.catchup.clear();			
		}

		virtual proto::paxos_ack paxos_accept(nodeconn::pointer conn, const proto::payload::paxos_accept& paxacc) override
		{
			auto log = nodecaps_->log("paxos-accept");

			std::lock_guard lock{ mutex_ };

			auto i = dataholder_.find(paxacc.meta.key);
			if(i != dataholder_.end())
			{
				//Use meta from the dataholder, because the meta of paxos_accept only contains key and paxos id.
				auto pack = this->accept(conn->id(), i->second.group_key, i->second.meta, i->second.body);

				_m_remove_from_queue(i->second.meta.internal, i->second.group_key, paxacc.meta.key);

				dataholder_.erase(i);
				return pack;
			}
			return proto::paxos_ack::failed;
		}

		virtual proto::paxos_ack paxos_rollback(const proto::payload::paxos_rollback& paxrol) override
		{
			auto proposal = core_.rollback(paxrol.meta.key);

			proto::paxos_ack ack = proto::paxos_ack::done;

			std::lock_guard lock{ mutex_ };

			if(!proposal.empty())
				ack = adapter_.rollback(proposal);

			taskque_.rollback(paxrol.meta.key);

			return ack;
		}

		virtual void paxos_catchup(nodeconn::pointer conn, const proto::payload::paxos_catchup& paxcth) override
		{
			if(paxcth.done)
				_m_catch_me_done(conn);
			else
				_m_catch_me_up(conn, paxcth);
		}

		virtual void paxos_catchup_data(nodeconn::pointer conn, const proto::payload::paxos_catchup_data& paxcthdata, std::shared_ptr<buffer> data) override
		{
			_m_catchup_data(conn, paxcthdata, data);
		}

		virtual void paxos_catchup_accepted(nodeconn::pointer conn, const proto::payload::paxos_catchup_accepted& paxacc) override
		{
			//The remote node became ready, it tells this node to catchup now
			auto me = nodecaps_->identity();
			auto remote = conn->id();

			core_.enable_node_ready(conn->id(), "Catchup by node("+remote+")");

			_m_catchup(conn, false);
		}
	private:
		// Overrides the recv_interface
		virtual void online_changed(nodeconn::pointer conn, online_status os) override
		{
			if(online_status::offline != os)
				return;

			std::lock_guard lock{ mutex_ };

			auto removes = delay_.requests.offline(conn);
			_m_remove_queued_keys(removes);

			auto i = delay_.catchup.find(conn->id());
			if(i != delay_.catchup.cend())
			{
				i->second.erase(conn);
				if(i->second.empty())
					delay_.catchup.erase(i);
			}
		}
	public:
		// Overrides the acceptor_interface

		std::unique_ptr<abs::preparer_interface> prepare(const proto::payload::paxos_meta& meta, const std::string& body)
		{
			std::unique_ptr<preparer_impl> prep{ new preparer_impl{this, meta.internal, meta.key} };

			std::lock_guard lock{ mutex_ };

			auto& req = dataholder_[meta.key];
			req.timestamp = std::time(nullptr);
			req.meta.key = meta.key;

			if (meta.internal)
			{
				internal_queue_.push_back(&req);
				if (internal_queue_.size() > 1)
					return prep;
			}
			else
			{

				req.group_key = adapter_.grouping_key ? adapter_.grouping_key(body) : std::string{};

				prep->grouping_key(req.group_key);

				if(req.group_key.empty())
					return prep;

				auto& queue = table_[req.group_key];

				queue.push_back(&req);

				if (queue.size() > 1)
					return prep;
			}

			proposer_->head_proposal_changed(meta.internal, prep->grouping_key(), meta.key);

			return prep;
		}

		proto::paxos_ack accept(const std::string& sender, const std::string& group_key, const proto::payload::paxos_meta& meta, const std::string& data)
		{
			std::lock_guard lock{ mutex_ };

			//Before accepting this request, it writes the request to the disk
			taskque_.write(sender, meta, data);

			//Check if it is satisfied with the condition of generating snapshot
			if(taskque_.size() > 100000)
			{
				nodecaps_->exec().run([this]{
					_m_make_snapshot();
				});
			}

			proto::paxos_ack ack;

			if(meta.internal)
				//Perform the internal request
				ack = (_m_perform_internal_req(std::string_view{data.data(), data.size()}) ?
					proto::paxos_ack::done:
					proto::paxos_ack::failed);
			else
				ack = adapter_.accept(data);

			if(proto::paxos_ack::done == ack)
				core_.register_rollback(group_key, meta.key, data);

			return ack;
		}
	private:
		static int _m_comp_latest(const std::map<std::string, proto::payload::paxos_meta>& a, const std::map<std::string, proto::payload::paxos_meta>& b)
		{
			std::size_t num_a = 0;
			std::size_t num_b = 0;

			for(auto & ca : a)
			{
				auto i = b.find(ca.first);
				if(i == b.end())
					continue;

				if(ca.second.time > i->second.time || (ca.second.time == i->second.time && ca.second.time_seq > i->second.time_seq))
					++num_a;
			}

			for(auto & cb : b)
			{
				auto i = a.find(cb.first);
				if(i == a.end())
					continue;

				if(cb.second.time > i->second.time || (cb.second.time == i->second.time && cb.second.time_seq > i->second.time_seq))
					++num_b;
			}

			if(num_a && num_b)
				return -1;	//conflict
			else if(num_a)
				return 1;
			else if(num_b)
				return 2;

			return 0;
		}

		/// Elects a node to be ready
		/**
		 *  When the whole cluster is restarting, all nodes are still in unready state after applying
		 *  the local tasks. The nodes have to elect a node to be ready.
		 */
		void _m_elect_ready(const std::set<std::string>& tried_nodes)
		{
			auto const me = nodecaps_->identity();
			
			auto nodes = core_.idle_nodes();
			if(nodes.empty() || (nodes.size() != tried_nodes.size()))
				return;

			paxos_core::stat_type s;
			auto active_nodes = core_.active_nodes(s);
			if(!active_nodes.empty())
				return;

			std::string nodes_str;
			for(auto & endpt : nodes)
			{
				if(tried_nodes.count(endpt.id) == 0)
					return;

				nodes_str += endpt.id + ", ";
			}

			core_.state(paxos_core::running_states::pending, true);

			for(auto & endpt : nodes)
			{
				if(!(me < endpt.id))
					return;
			}

			//Check task anchors of other nodes, and elect the node who has the lastest task queue.

			std::vector<nodeconn::pointer> remote_nodes;

			for(auto & endpt : nodes)
			{
				auto conns = nodecaps_->find(endpt.id);
				if(conns.empty())
					return;

				remote_nodes.push_back(*conns.cbegin());
			}

			using cursor_table = std::map<std::string, proto::payload::paxos_meta>;

			auto latest = std::make_shared<std::pair<std::string, cursor_table>>();

			latest->first = me;
			latest->second = this->cursors();

			//first: the number of responses. second: the number of conflicts
			auto comp_count = std::make_shared<std::pair<std::size_t, std::size_t>>(0, 0);

			for(auto & conn : remote_nodes)
			{
				auto node = conn->id();
				std::size_t count = remote_nodes.size();

				core_.paxos_cursors(conn, [this, me, node, latest, count, comp_count](std::optional<cursor_table>& cursors){
					
					std::lock_guard lock{ mutex_ };
					++(comp_count->first);
					int val = -100;
					if(cursors)
					{
						val = _m_comp_latest(latest->second, cursors.value());

						if(-1 == val)
							++(comp_count->second);
						else if(2 == val)
						{
							latest->first = node;
							latest->second = cursors.value();
						}
					}

					if(comp_count->first == count && 0 == comp_count->second)
					{
						if(latest->first == nodecaps_->identity())
						{
							core_.state(paxos_details::paxos_core::running_states::ready, true);
							if(signals.on_ready)
								signals.on_ready();
						}
						else
						{
							auto conns = nodecaps_->find(latest->first);
							if(!conns.empty())
							{
								auto result = core_.paxos_ready(*conns.cbegin(), 2 /*electing*/, [](std::size_t s){
								});
							}
						}
					}
				});
			}

		
		}

		/// Checks if the paxos is ready on the node specified by the connection.
		void _m_paxos_exists(nodeconn::pointer conn, std::shared_ptr<std::set<std::string>> tried_nodes)
		{
			auto me = nodecaps_->identity();

			tried_nodes->insert(conn->id());
			proto::payload::paxos_exists paxexs;
			paxexs.paxos = paxos_;

			conn->send(this, paxexs, [this, conn, tried_nodes](std::shared_ptr<buffer> buf, std::error_code err){
				auto log = nodecaps_->log(paxos_, "paxos_exists");

				auto me = nodecaps_->identity();

				if((!err) && (buf->pkt()->api_key == proto::api::ack))
				{
					if((1 << 15) == buf->pkt()->len)
						//Error on remote node
						return;

					auto const ack = buf->pkt()->len;

					if(0 == ack)
						//Remote node is not the paxos node
						return;

					if(0 == (ack & 4))
					{
						//This node is not the paxos node. Destory itself.
						log.msg("This node is not paxos(\"", paxos_, "\") node.");
						nodecaps_->delete_paxos(paxos_);
						return;
					}
					else if(ack & 2)
					{
						//This is the paxos node and remote node is ready
						log.msg("This is the paxos(\"", paxos_, "\") node. Catchup now");

						core_.enable_node_ready(conn->id(), "paxos_exists: this node and remote node are ready");

						//std::this_thread::sleep_for(std::chrono::seconds{ 15 });

						if(core_.state() != paxos_core::running_states::ready)
						{
							_m_catchup(conn, false);
							log.msg("Catchup has been called");
						}
						else
						{
							log.msg("Skip catchup because this node is ready");
						}
						return;						
					}
					else
					{
						//This is the paxos node and remote node is not ready
						_m_elect_ready(*tried_nodes);
					}
				}
				else
					log.err("paxos(\"", paxos_, "\") failed to check paxos_exists");

				_m_join(tried_nodes);
			});
		}

		void _m_paxos_endpoint_for_join()
		{
			nodecaps_->exec().run([this] {
				std::this_thread::sleep_for(std::chrono::seconds{ 1 });

				core_.paxos_endpoint([this](bool updated) {
					_m_join();
					});
			});
		}

		/// Joins the paxos
		/**
		 * Method join is to reconnect/participate to the paxos instance. Before joining the paxos
		 * it examines if this node is still a one of paxos nodes. If it is, the paxos catches up the 
		 * latest states. If it is not, it destroies itself.
		 */
		void _m_join(std::shared_ptr<std::set<std::string>> tried_nodes = nullptr)
		{
			//auto me = nodecaps_->identity();

			if(!tried_nodes)
				tried_nodes = std::make_shared<std::set<std::string>>();
			
			std::lock_guard lock{ mutex_ };
			auto endpts = core_.inactive_nodes(false);
			if(endpts.empty())
			{
				auto conn = core_.random_pick_idle(*tried_nodes);
				if(conn)
				{
					nodecaps_->log(paxos_, "_m_join").msg("Check paxos existing on idle node(",conn->id(),")");
					_m_paxos_exists(conn, tried_nodes);
				}
				return;
			}

			if(tried_nodes->size() == endpts.size())
			{
				_m_paxos_endpoint_for_join();
				return;
			}

			for(auto & endpt : endpts)
			{
				if(tried_nodes->count(endpt.id))
					continue;

				tried_nodes->insert(endpt.id);


				if(endpt.addr.empty())
				{
					_m_paxos_endpoint_for_join();
				}
				else
				{
					nodecaps_->make_nodeconn(endpt.addr, [this, tried_nodes](nodeconn::pointer conn, bool new_created){
						if(conn)
						{
							nodecaps_->log(paxos_, "_m_join").msg("connected to node(",conn->id(), ")");
							_m_paxos_exists(conn, tried_nodes);
						}
						else
							_m_join(tried_nodes);

						//Don't retry to connect the remote node if it fails
						return false;
					});
				}

				return;
			}
		}

		/// Adds paxos_internal_insert of me to the local queue when this node is first node of paxos instance
		/**
		 * A paxos_internal_insert is inserted to the queue if the queue is first time initialized. When a node is
		 * invited by another node, the new node can get this node by performing catchup. If the first node doesn't add
		 * itself into the queue, the new node would miss this node.
		 */
		void _m_paxos_insert_me()
		{
			proto::payload::paxos_internal_insert paxins;
			paxins = nodecaps_->endpoint();

			std::string body;

			body.resize(1 + paxins.bytes());

			*reinterpret_cast<proto::paxos_internal*>(body.data()) = proto::paxos_internal::insert;
			paxins.serialize(body.data() + 1);

			auto meta = proposer_->make_paxos_meta(true, body);

			std::lock_guard lock{ mutex_ };
			if(taskque_.empty())
			{
				//It's the first node of the paxos instance, so it's status must be ready.
				//core_.ready(true);
				core_.state(paxos_core::running_states::ready, false);
				taskque_.write(nodecaps_->identity(), meta, body);

				if(signals.on_ready)
					signals.on_ready();
			}
		}

		//This method is provided for task queue, it runs the local queue tasks for rebulding the states.
		bool _m_apply_request(const identifier& key, bool internal, std::string_view body)
		{
			if(internal)
				return _m_perform_internal_req(body);
			
			std::lock_guard lock{ mutex_ };
			return (proto::paxos_ack::done == adapter_.accept(body));
		}

		bool _m_perform_internal_req(std::string_view body)
		{
			auto log = nodecaps_->log("acceptor-internal-req");

			auto cmd = *reinterpret_cast<const proto::paxos_internal*>(body.data());

			if(proto::paxos_internal::insert == cmd)
			{
				proto::payload::endpoint endpt;
				std::size_t len = body.size() - 1;
				if(endpt.deserialize(body.data() + 1, len))
				{
					core_.enable_paxos(endpt);
					return true;
				}
			}

			return false;
		}

		bool _m_is_first(bool internal, const std::string& grouping_key, const identifier& key) const
		{
			if (internal)
			{
				if(internal_queue_.size())
					return (internal_queue_.front()->meta.key == key);
			}
			else
			{
				//Assume the proposal always be first if table-key is empty
				if(grouping_key.empty())
					return true;

				auto i = table_.find(grouping_key);
				if (i != table_.cend())
				{
					if(i->second.size())
						return (i->second.front()->meta.key == key);
				}
			}
		
			return false;
		}

		std::optional<identifier> _m_remove_from_queue(bool internal, const std::string& grouping_key, const identifier& key)
		{
			paxos_details::acceptor::request_data* next_req = nullptr;
			//The task is internal task if grouping_key is empty.
			if (internal)
			{
				for (auto i = internal_queue_.cbegin(); i != internal_queue_.cend(); ++i)
				{
					if ((*i)->meta.key == key)
					{
						internal_queue_.erase(i);
						if(!internal_queue_.empty())
							next_req = internal_queue_.front();
						break;
					}
				}
			}
			else
			{
				if(grouping_key.empty())
					return {};

				auto i = table_.find(grouping_key);
				if (i != table_.cend())
				{
					auto& queue = i->second;

					if (!queue.empty())
					{
						for (auto u = queue.cbegin(); u != queue.cend(); ++u)
						{
							if ((*u)->meta.key == key)
							{
								queue.erase(u);
								if(!queue.empty())
									next_req = queue.front();
								break;
							}
						}
					}
				}
			}

			if (next_req)
			{
				proposer_->head_proposal_changed(internal, grouping_key, next_req->meta.key);

				delay_.requests.paxos_propose_resp(next_req->meta.key, proto::paxos_ack::ready);
				return next_req->meta.key;
			}

			//No proposal in the front of queue
			proposer_->head_proposal_changed(internal, grouping_key, {});
			return {};
		}

		/// Catches up the cluster queue
		void _m_catchup(nodeconn::pointer conn, bool done)
		{
			if(done)
			{
				core_.state(paxos_core::running_states::ready, false);

				core_.paxos_notify_ready(nodecaps_->identity(), [this, conn]{
					auto log = nodecaps_->log("acceptor-catchup");
					proto::payload::paxos_catchup paxcth;
					paxcth.paxos = paxos_;
					paxcth.done = true;
					paxcth.internal_off = 0;
					paxcth.external_off = 0;
					paxcth.anchors_off = 0;

					conn->send(paxcth);

					log.msg("Sending paxos_catchup(\"", paxos_ ,"\") done");
				});
				return;
			}

			// Sends the cursors to the remote node and catches the queue up from the cursors postions.
			auto cursors = taskque_.cursors();

			proto::payload::paxos_catchup paxcth;
			paxcth.paxos = paxos_;
			paxcth.done = false;
			paxcth.internal_off = 0;
			paxcth.external_off = 0;
			paxcth.anchors_off = 0;

			if(cursors.empty())
			{
				// The snapshot may not be completely
				auto soff = taskque_.snapshot_offsets();
				if(soff)
				{
					paxcth.internal_off = soff->internal;
					paxcth.external_off = soff->external;
					paxcth.anchors_off = soff->anchors;
				}
			}
			else
			{
				for(auto & mycur : cursors)
				{
					auto & cur = paxcth.cursors.emplace_back();

					cur.node = mycur.first;
					cur.key = mycur.second.key;
					cur.time = mycur.second.time;
					cur.time_seq = mycur.second.time_seq;
				}
			}

			//Catches up the queue of remote node which is specified by conn.
			conn->send(paxcth);
		}

		/// A remote node request to catch me up
		void _m_catch_me_up(nodeconn::pointer conn, const proto::payload::paxos_catchup& paxcth)
		{
			auto log = nodecaps_->log("acceptor-catch_me_up");
			auto const me = nodecaps_->identity();

			std::shared_ptr<catcher> catcher_ptr;

			std::lock_guard lock{ mutex_ };
			auto i = catchers_.find(conn);
			if(i == catchers_.cend())
			{
				catcher_ptr = std::make_shared<catcher>(taskque_);
				catchers_[conn] = catcher_ptr;
			}
			else
				catcher_ptr = i->second;

			bool completed = false;
			auto cdata = catcher_ptr->catchup_data(paxcth, completed);

			using catchup_status = proto::payload::paxos_catchup_data_status;
			proto::payload::paxos_catchup_data paxcth_data;
			paxcth_data.paxos = paxos_;

			if(!catcher_ptr->paused())
			{
				if(completed)
				{
					if(!core_.pause(true))
					{
						paxcth_data.status = catchup_status::refused;
						conn->send(paxcth_data);
						return;
					}

					//Now the paxos cluster is paused
					catcher_ptr->set_pause();

					auto count = taskque_.catchup_data_size(paxcth.cursors);

					if(count)
					{
						if(cdata.value().task_size() == count.value())
							paxcth_data.status = catchup_status::last;
						else
							paxcth_data.status = catchup_status::normal;
					}
					else
						paxcth_data.status = catchup_status::refused;
				}
			}
			else
				paxcth_data.status = completed ? catchup_status::last : catchup_status::normal;

			if(cdata.value().bytes())
			{
				auto p = reinterpret_cast<const char*>(cdata.value().data());
				paxcth_data.bufview = std::string_view{p, cdata.value().bytes()};

				conn->send(paxcth_data, p,  cdata.value().bytes());
			}
			else
				conn->send(paxcth_data);
		}

		void _m_catch_me_done(nodeconn::pointer conn)
		{
			bool paused = false;
			{
				std::lock_guard lock{ mutex_ };

				auto i = catchers_.find(conn);
				if(i == catchers_.cend())
					return;

				paused = i->second->paused();
			}

			if(paused)
				core_.pause(false);

			std::lock_guard lock{ mutex_ };
			catchers_.erase(conn);
		}

		/// Receives the catchup data
		void _m_catchup_data(nodeconn::pointer conn, const proto::payload::paxos_catchup_data& paxcthdata, std::shared_ptr<buffer> buf)
		{
			auto log = nodecaps_->log("acceptor-catchup_data");

			if(static_cast<std::uint8_t>(paxcthdata.status) < 3)
			{
				const char* status_text[] = {"normal", "refused", "last"};
				log.msg("paxos(\"", paxos_ , "\") status = ", status_text[static_cast<std::uint8_t>(paxcthdata.status)]);
			}
			else
				log.msg("paxos(\"", paxos_, "\")  status = invalid status");

			auto me = nodecaps_->identity();

			{
				//the acceptor's mutex is required, because taskqueue_.write_rawdata callbacks the acceptor's method
				//which acquires the acceptor's mutex.
				std::lock_guard lock{ mutex_ };
				taskque_.write_rawdata(paxcthdata.bufview);
			}

			if(proto::payload::paxos_catchup_data_status::last == paxcthdata.status)
			{
				taskque_.reset_write_state();
				//Catchup is done
				_m_catchup(conn, true);
				return;
			}

			_m_catchup(conn, false);
		}

		void _m_make_snapshot()
		{
			std::lock_guard lock{ mutex_ };
			taskque_.update_snapshot([this](std::filesystem::path internal_path, std::filesystem::path external_path){
				//locqueue is acquiring to save the states into the specified internal file
				core_.serialize_state(internal_path);
				adapter_.serialize(external_path);
			});
		}

		void _m_remove_queued_keys(const std::vector<identifier>& keys)
		{
			for(auto & key : keys)
			{
				auto i = dataholder_.find(key);
				if(i == dataholder_.cend())
					continue;

				_m_remove_from_queue(i->second.meta.internal, i->second.group_key, key);
			}
		}
	private:
		std::string const paxos_;	///< ID of this paxos instance
		nodecaps_interface* const nodecaps_;
		paxos_core & core_;
		queue_invoker invoker_;
		task_queue	taskque_;		///< Local serialized queue.
		abs::proposer_interface* proposer_{ nullptr };

		adapter_type adapter_;

		mutable std::recursive_mutex	mutex_;

		std::deque<request_data*> internal_queue_;
		std::map<std::string, std::deque<request_data*>> table_;

		std::map<identifier, request_data> dataholder_;	///< Key data holder
		std::map<nodeconn::pointer, std::shared_ptr<catcher>> catchers_;

		struct delays
		{
			conflicted_request requests;
			std::map<std::string, std::set<nodeconn::pointer>> catchup;

			delays(acceptor& self):
				requests(self)
			{}
		}delay_;
	};
}

#endif