#ifndef PIPASHAN_PAXOS_PROPOSER_INCLUDED
#define PIPASHAN_PAXOS_PROPOSER_INCLUDED
#include "acceptor.hpp"
#include "../transfer.hpp"
#include "../nodecaps.hpp"
#include "../modules/md5.hpp"
#include "paxos_core.hpp"
#include "fixer.hpp"

namespace pipashan::paxos_details
{
	class proposer:
		public net::nodeconn::paxos_proposer_interface,
		public abs::proposer_interface
	{
	public:
		using nodecaps_interface = pipashan::abs::nodecaps_interface;
		using nodeconn = net::nodeconn;
		using buffer = proto::buffer;

		enum class record_notify
		{
			normal,
			accepted_required,
			forcely
		};

		struct proposal_record:
			public net::nodeconn::recv_interface
		{
			proposer* self;
			std::string grouping_key;
			std::mutex condvar_mutex;
			std::condition_variable condvar;

			bool notified{ false };
			bool conflict_fixed{ false };	///< Determines this proposal is going to be accepted by conflict fix

			std::set<nodeconn::pointer> nodes;
			std::set<std::string> ready;
			std::set<std::string> conflicted;
			std::set<std::string> failed;
			std::set<std::string> offline;

			void try_notify(record_notify notify)
			{
				if(record_notify::forcely != notify)
				{
					std::lock_guard lock{ self->mutex_ };

					if (failed.empty() && offline.empty())
					{
						if (ready.size() + conflicted.size() != nodes.size() + 1)
							return;

						if (!((record_notify::accepted_required == notify) && self->_m_is_accepted(ready.size())))
							return;
					}
				}

				std::lock_guard lock{ condvar_mutex };
				notified = true;
				condvar.notify_one();
			}

			void wait()
			{
				std::unique_lock lock{ condvar_mutex };
				if(!notified)
					condvar.wait(lock);
			}
		private:
			virtual void established(nodeconn::pointer) override{}

			virtual void online_changed(nodeconn::pointer conn, online_status os) override
			{
				if (online_status::offline != os)
					return;

				{
					std::lock_guard lock{ self->mutex_ };

					offline.insert(conn->id());
				}

				try_notify(record_notify::normal);
			}

			virtual recv_status recv(nodeconn::pointer, std::shared_ptr<buffer>) override
			{
				return recv_status::success;
			}
		};

		proposer(const std::string& id, paxos_core& core, nodecaps_interface* nodecaps, acceptor& acc):
			nodecaps_(nodecaps),
			acceptor_(acc),
			core_(core),
			paxos_(id),
			fixer_(nodecaps)
		{
		}

		~proposer()
		{
			core_.clear();
		}

		bool paxos_internal_insert(const proto::payload::endpoint& endpt)
		{
			proto::payload::paxos_internal_insert paxins;
			paxins = endpt;

			std::string body;

			body.resize(1 + paxins.bytes());

			*reinterpret_cast<proto::paxos_internal*>(body.data()) = proto::paxos_internal::insert;
			paxins.serialize(body.data() + 1);

			return _m_propose(body, true);
		}

		bool insert(nodeconn::pointer conn)
		{
			if(core_.is_paxos_ready(conn, [](bool){}))
				return true;

			if(!propose_internal_insert(conn->endpt()))
				return false;

			core_.insert(conn, node_status::pending);

			return true;
		}

		bool propose(const std::string& body)
		{
			return _m_propose(body, false);
		}
	private:
		std::uint16_t _m_time_seq(std::time_t tm)
		{
			std::lock_guard lock{ mutex_ };
			if(tm == time_)
				return ++time_seq_;

			time_seq_ = 0;
			time_ = tm;
			return 0;
		}

		bool _m_is_accepted(std::size_t ready_size) const
		{
			auto stat = core_.stat();
			return (ready_size + stat.preparing_nodes >= (stat.size + 1) / 2 + 1);
		}

		template<typename InternalPayload>
		bool _m_propose(proto::paxos_internal appl_cmd, const InternalPayload& payload)
		{
			std::string request;
			request.resize(1 + payload.bytes());
			*reinterpret_cast<proto::paxos_internal*>(request.data()) = appl_cmd;
			
			payload.serialize(request.data() + 1);
			return _m_propose(request, true);
		}

		proposal_record* _m_send_proposal(const std::vector<nodeconn::pointer>& nodes, const std::string& grouping_key, const proto::payload::paxos_propose& paxpro, paxos_details::abs::preparer_interface& prep)
		{
#ifdef SHOWLOG_PROPOSE
			auto log = nodecaps_->log("_m_send_proposal");
#endif
			proposal_record* record;
			
			{
				std::lock_guard lock{ mutex_ };
				record = &proposals_[paxpro.meta.key];
				record->self = this;
				record->grouping_key = grouping_key;
			}

			for(auto & conn : nodes)
				record->nodes.insert(conn);

			prep.is_first([this, record](bool is_first) {
#ifdef SHOWLOG_PROPOSE
				auto log = nodecaps_->log("_m_send_proposal");
				log.msg("proposer local node is ", (is_first ? "ready" : "conflicted"));
#endif
				if(is_first)
					record->ready.insert(nodecaps_->identity());
				else
					record->conflicted.insert(nodecaps_->identity());
			});

			for (auto& conn : nodes)
			{
				conn->add(record);
				conn->send(paxpro);
			}

			record->wait();

			//Remove the record from the nodeconn objects.
			for (auto& conn : nodes)
				conn->erase(record, true);

			return record;
		}

		/// Proposes a data
		/**
		 * The frist parameter is a string object, it can avoid the issue caused by changing the body of argument during the proposing.
		 */
		bool _m_propose(std::string body, bool internal)
		{
			proto::payload::paxos_propose paxpro;

			paxpro.body.swap(body);
			paxpro.meta = make_paxos_meta(internal, paxpro.body);

			paxos_core::stat_type stat;
			auto nodes = core_.active_nodes(stat);

			//When the system is just initilized, there is not an acceptor added to the cluster, let the
			//application pass.
			if(nodes.empty() || core_.empty())
			{
				auto log = nodecaps_->log("paxos::propose");

				if(nodes.empty() && (stat.size != stat.preparing_nodes))
				{
					log.msg("no active nodes, stat: size = ",stat.size,", preparing_nodes = ",stat.preparing_nodes);
					return false;
				}

				auto group_key = acceptor_.grouping_key(paxpro.body);

				if(proto::paxos_ack::done != acceptor_.accept(nodecaps_->identity(), group_key, paxpro.meta, paxpro.body))
				{
					log.err("prepare failed - reactor didn't accept the request");
					return false;
				}
				return true;
			}

			{
				std::lock_guard lock{ mutex_ };
				fixer_.resize(nodes);
			}

			auto preparer = acceptor_.prepare(paxpro.meta, paxpro.body);

			bool propose_ready = true;

			{
				proposal_record * record = _m_send_proposal(nodes, preparer->grouping_key(), paxpro, *preparer);
			
				std::lock_guard lock{ mutex_ };

				if(!record->conflict_fixed)
					propose_ready = record->failed.empty() && _m_is_accepted(record->ready.size());

				proposals_.erase(paxpro.meta.key);
			
			}

			if(!propose_ready)
			{
				/// The number of nodes that responded ack ready is less than the required value.
				proto::payload::paxos_cancel paxcan;
				paxcan.meta.paxos = paxos_;
				paxcan.meta.key = paxpro.meta.key;

				if(!nodes.empty())
				{
					//Send paxos_cancel to the nodes that responded ack ready.
					make_transfer(nodes, nodecaps_->nodelog()).send(paxcan, [this, &nodes](nodeconn::pointer conn, std::shared_ptr<buffer> buf, const std::error_code& err){
						if((!err) && (proto::api::paxos_ack == buf->pkt()->api_key))
						{
							proto::payload::paxos_ack paxack;
							if(buf->deserialize(paxack))
							{
								if(proto::paxos_ack::done == paxack.ack)
									return;
							}
						}

						std::erase(nodes, conn);
					});

					if(!_m_is_accepted(nodes.size() + 1))
					{

					}
				}
				auto log = nodecaps_->log("paxos::propose");
				log.msg("propose interrupted - acceptors are not ready. ", nodes.size(), " acceptors responded ready");

				return false;
			}

			if (proto::paxos_ack::done != acceptor_.accept(nodecaps_->identity(), preparer->grouping_key(), paxpro.meta, paxpro.body))
			{
				auto log = nodecaps_->log("paxos::propose");
				log.err("prepare failed - reactor didn't accept the request");
				return false;
			}

			paxpro.body.clear();
			
			//Meta in paxos_accept only contains paxos id and key.
			proto::payload::paxos_accept paxacc;
			paxacc.meta.paxos = paxos_;
			paxacc.meta.key = paxpro.meta.key;

			std::atomic<std::size_t> ack_done{ 0 };

			{
				std::lock_guard lock{ mutex_ };
				fixer_.begin_accept(internal, preparer->grouping_key(), paxpro.meta.key);
			}

			auto node_size = make_transfer(nodes, nodecaps_->nodelog()).send(paxacc, [this, &ack_done](nodeconn::pointer conn, std::shared_ptr<buffer> buf, const std::error_code& err){
#ifdef SHOWLOG_PAXOS_ACCEPT_HANDLER
					auto log = nodecaps_->log("paxos-accept-handler");
					if(err)
						log.msg("node(", conn->short_id(),") socket error");
					else
						log.msg("node(", conn->short_id(),")  packet = ", to_string(buf->pkt()->api_key));
#endif
					if((!err) && (proto::api::paxos_ack == buf->pkt()->api_key))
					{
						proto::payload::paxos_ack paxack;
						if(buf->deserialize(paxack))
						{
							if(proto::paxos_ack::done == paxack.ack)
							{
								++ack_done;
								return;
							}
						}
#ifdef SHOWLOG_PAXOS_ACCEPT_HANDLER
						else
							log.err("node(", conn->short_id(), ") paxos-accept failed to deserialize");
#endif
					}
			});


			{
				std::lock_guard lock{ mutex_ };
				fixer_.end_accept(internal, preparer->grouping_key(), paxpro.meta.key);
			}

			//msg<<"paxos_accept finished. dones="<<dones.size()<<", acceptors="<<node_size<<std::endl;

			if(ack_done == node_size)
			{
				return true;
			}

			proto::payload::paxos_rollback paxrol;
			paxrol.meta.paxos = paxos_;
			paxrol.meta.key = paxpro.meta.key;

			std::set<nodeconn::pointer> dones;

			make_transfer(nodes, nodecaps_->nodelog()).send(paxrol, [&dones](nodeconn::pointer conn, std::shared_ptr<buffer> buf, const std::error_code& err){
					if((!err) && (proto::api::paxos_ack == buf->pkt()->api_key))
					{
						proto::payload::paxos_ack paxack;
						if(buf->deserialize(paxack))
						{
							if(proto::paxos_ack::done == paxack.ack)
								dones.insert(conn);
						}
					}
			});

			acceptor_.paxos_rollback(paxrol);

			auto log = nodecaps_->log("paxos::propose");
			log.err("propose interrupted - acceptors are failed to accept it");

			return false;
		}
	public:
		virtual void online_changed(nodeconn::pointer conn, net::online_status os) override
		{
			if (net::online_status::offline == os)
			{
				auto log = nodecaps_->log("proposer");
				std::lock_guard lock{ mutex_ };
				for (auto& prop : proposals_)
				{
					auto i = prop.second.nodes.find(conn);
					if (i == prop.second.nodes.cend())
						continue;

					prop.second.nodes.erase(i);
					prop.second.ready.erase(conn->id());
					prop.second.conflicted.erase(conn->id());

					log.msg("notify proposal(", prop.first.short_hex(),") because node(",conn->id(),") offlined");

					prop.second.try_notify(record_notify::forcely);
				}

			}
		}

		virtual void paxos_propose_resp(nodeconn::pointer conn, const proto::payload::paxos_propose_resp& paxrsp) override
		{
			std::lock_guard lock{ mutex_ };

			auto i = proposals_.find(paxrsp.meta.key);
			if (i == proposals_.cend())
				return;

#ifdef SHOWLOG_PROPOSE
			auto log = nodecaps_->log("recv-paxos-propose-resp");
			log.msg("node(", conn->id(), ") key=", paxrsp.meta.key.short_hex(), " first-key=", (paxrsp.first ? paxrsp.first->short_hex() : "empty"));
#endif
			auto record = &i->second;

			switch (paxrsp.ack)
			{
			case proto::paxos_ack::ready:
				record->conflicted.erase(conn->id());
				record->ready.insert(conn->id());
				break;
			case proto::paxos_ack::conflicted:
				//class nodeconn always processes a paxos in different threads immediately, so that, the
				//ack(conflicted) maybe processes after ack(ready) because of thread scheduler/order of
				//acquiring of the mutex and so on. Therefore, before setting the conflicted, we have to
				//check the node if it is already ack(ready).

				//Ignore the conflicted if it is ready
				if(0 == record->ready.count(conn->id()))
				{
					record->conflicted.insert(conn->id());
					//log.msg(paxrsp.meta.key.short_hex(), ":conflicted from node(", conn->short_id(),")");
				}
				break;
			case proto::paxos_ack::failed:
				record->failed.insert(conn->id());
				//log.msg(paxrsp.meta.key.short_hex(), ":failed from node(", conn->short_id(),")");
				break;
			default:
				break;
			}

			auto notify = record_notify::accepted_required;

			if(paxrsp.meta.internal || !i->second.grouping_key.empty())
			{
				auto winner = fixer_.update(conn->id(), paxrsp.meta.internal, i->second.grouping_key, paxrsp.first);
				if(winner)
				{
#ifdef SHOWLOG_PROPOSE
					log.msg("conflict detected: wake proposal:", winner->short_hex());
#endif
					//A conflict is detected
					if(*winner != paxrsp.meta.key)
					{
						//Wakes up the record
						auto u = proposals_.find(winner.value());
						if (u != proposals_.cend())
						{
							u->second.conflict_fixed = true;
							u->second.try_notify(record_notify::forcely);
						}
						return;
					}
					else
					{
						record->conflict_fixed = true;
						notify = record_notify::forcely;
					}
				}
			}

			record->try_notify(notify);
		}
	public:
		// Overwrite the proposer_interface
		virtual proto::payload::paxos_meta make_paxos_meta(bool internal, const std::string& data) override
		{
			proto::payload::paxos_meta meta;
			meta.time = std::time(nullptr);
			meta.time_seq = _m_time_seq(meta.time);

			meta.key.hash(paxos_, nodecaps_->identity(), meta.time, meta.time_seq);

			meta.internal = internal;
			meta.paxos = paxos_;
			return std::move(meta);
		}

		virtual bool propose_internal_insert(const proto::payload::endpoint& endpt) override
		{
			return paxos_internal_insert(endpt);		
		}

		virtual void activate_proposal(const identifier& key) override
		{
			std::lock_guard lock{ mutex_ };
			auto i = proposals_.find(key);
			if(i == proposals_.cend())
				return;

#ifdef SHOWLOG_PROPOSE
			auto log = nodecaps_->log("activate-proposal");
			log.msg("key:",key.short_hex());
#endif
			i->second.conflicted.erase(nodecaps_->identity());
			i->second.ready.insert(nodecaps_->identity());
			i->second.try_notify(record_notify::accepted_required);
		}

		virtual void head_proposal_changed(bool internal, const std::string& grouping_key, const std::optional<identifier>& key) override
		{
#ifdef SHOWLOG_PROPOSE
			auto log = nodecaps_->log("head-proposal-changed");
			log.msg("internal=",internal, ", table-key=", grouping_key, ", key=", key ? key->short_hex() : "empty");
#endif

			std::lock_guard lock{ mutex_ };
			auto winner = fixer_.update(internal, grouping_key, key);
			if(winner)
			{
				//A conflict is detected, so wakes the record up
				auto u = proposals_.find(winner.value());
				if (u != proposals_.cend())
				{
					u->second.conflict_fixed = true;
					u->second.try_notify(record_notify::forcely);
				}
			}
		}
	private:
		nodecaps_interface * const nodecaps_;
		acceptor& acceptor_;
		const std::string paxos_;				///< ID of paxos instance
		std::atomic<std::size_t> seq_{ 0 };

		paxos_core& core_;

		mutable std::recursive_mutex mutex_;

		std::time_t time_{ 0 };
		std::uint16_t time_seq_{ 0 };

		std::map<identifier, proposal_record> proposals_;

		fixer fixer_;	///< Conflict fixer
	};
}

#endif