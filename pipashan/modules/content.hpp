#ifndef PIPASHAN_CONTENT_INCLUDED
#define PIPASHAN_CONTENT_INCLUDED

#include "../paxos/acceptor.hpp"
#include "../paxos/proposer.hpp"
#include "../paxos/paxos.hpp"
#include "../execute.hpp"
#include "../nodecaps.hpp"
#include <json.hpp>
#include <shared_mutex>

namespace pipashan
{
	namespace content_proto
	{
		enum class actions: std::uint8_t
		{
			create,
			destroy,

			insert,
			remove,

			node_refcnt,
		};

		inline actions act_from_body(std::string_view body)
		{
			return *reinterpret_cast<const actions*>(body.data());
		}

		template<typename Payload>
		std::string make(actions act, const Payload& payld)
		{
			std::string data;

			data.resize(sizeof(actions) + payld.bytes());

			*reinterpret_cast<actions*>(data.data()) = act;

			payld.serialize(data.data() + sizeof(actions));
			return data;
		}

		inline std::string make(actions act, const std::string& data)
		{
			std::string buf;

			buf.resize(sizeof(actions));

			*reinterpret_cast<actions*>(buf.data()) = act;

			buf += data;
			return buf;
		}

		struct create
		{
			std::string name;	///< Group name

			friend serialization& operator<<(serialization& s13n, const create& arg)
			{
				s13n.serialize(arg.name);
				return s13n;
			}

			friend deserialization& operator>>(deserialization& d15n, create& arg)
			{
				d15n.deserialize(arg.name);
				return d15n;
			}
		};

		struct insert
		{
			proto::payload::endpoint endpt;

			std::size_t bytes() const noexcept
			{
				return endpt.bytes();
			}

			void serialize(char* data) const noexcept
			{
				endpt.serialize(data);
			}

			bool deserialize(const char* data, std::size_t len)
			{
				return (endpt.deserialize(data, len) != nullptr);
			}
		};

		struct remove
		{
			std::string identity;

			std::size_t bytes() const noexcept
			{
				return 1 + identity.size();
			}

			void serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(identity.size());
				std::memcpy(data + 1, identity.data(), identity.size());
			}

			bool deserialize(const char* data, std::size_t len)
			{
				if(len < 1) return false;
				auto idsz = proto::as_size_t(data);

				if(idsz + 1 > len) return false;

				identity.assign(data + 1, idsz);
				return true;
			}
		};
	}

	enum class create_group_status: std::uint8_t
	{
		failed,
		success,
		existing
	};

	inline std::string to_string(create_group_status s)
	{
		switch(s)
		{
		case create_group_status::failed:
			return "failed";
		case create_group_status::success:
			return "success";
		case create_group_status::existing:
			return "exsiting";
		default:
			break;
		}

		return "unknown";
	}

	/// class content
	class content
	{
	public:
		using endpoint = proto::payload::endpoint;
		using nodeconn = net::nodeconn;

		struct node
		{
			endpoint endpt;
			std::set<nodeconn::pointer> conns;
		};

		struct group
		{
			std::map<std::string, node> nodes;
		};

		struct{
			std::function<void()> on_ready;
		}signals;

		content(abs::nodecaps_interface* nodecaps, std::size_t working_node_size, nodeconn::pointer inviter):
			nodecaps_(nodecaps),
			paxos_("content", working_node_size, nodecaps, inviter)
		{
			paxos_.signals.on_ready = [this]{
				if(signals.on_ready)
					signals.on_ready();
			};

			_m_make_acceptor();
		}

		bool ready() const noexcept
		{
			return paxos_.ready();
		}

		void make_ready(bool electing)
		{
			paxos_.make_ready(electing);
		}

		bool exists(const std::string& name) const
		{
			std::lock_guard lock{ mutex_ };
			return (groups_.count(name) != 0);
		}

		create_group_status create_group(const std::string& name)
		{
			{
				std::lock_guard lock{ mutex_ };

				if(groups_.count(name) != 0)
					return create_group_status::existing;
			}

			serialization s11n;
			s11n.serialize(content_proto::actions::create, name);

			if(!paxos_.propose(s11n.data()))
			{
				return create_group_status::failed;
			}

			std::lock_guard lock{ mutex_ };

			groups_[name];

			return create_group_status::success;
		}

		bool delete_group(const std::string& name)
		{
			{
				std::lock_guard lock{ mutex_ };

				if(groups_.count(name) == 0)
					return true;
			}

			serialization s11n;
			s11n.serialize(content_proto::actions::destroy, name);

			if(!paxos_.propose(s11n.data()))
				return false;

			std::lock_guard lock{ mutex_ };

			groups_.erase(name);

			return true;
		}

		/// Returns the all nodes which are added to this paxos instance, including this node.
		std::vector<endpoint> nodes() const
		{
			// The paxos returns the nodes without itself.
			auto remote_nodes = paxos_.nodes();
			remote_nodes.emplace_back(nodecaps_->endpoint());
			return remote_nodes;
		}

		paxos& paxos_object()
		{
			return paxos_;
		}
	private:
		void _m_make_acceptor()
		{
			adapter_type adapter;

			adapter.grouping_key = [this](std::string_view body){
				return "x";
			};

			adapter.accept = [this](std::string_view body){
				auto log = nodecaps_->log("content-reactor-accept");

				if(body.size() < sizeof(content_proto::actions))
				{
					log.err("invalid content body");
					return proto::paxos_ack::failed;
				}

				deserialization d15n{std::string_view{body.data() + 1, body.size() - 1}};

				auto act = content_proto::act_from_body(body);
				if(content_proto::actions::create == act)
				{
					std::string name;
					if(!d15n.deserialize(name))
						return proto::paxos_ack::failed;
					
					std::lock_guard lock{ mutex_ };
					groups_[name];
					return proto::paxos_ack::done;
				}
				else if(content_proto::actions::destroy == act)
				{
					log.msg("content_proto::actions::destory");

					std::string name;
					if(!d15n.deserialize(name))
					{
						log.err("content_proto::actions::destory failed to deserialize");
						return proto::paxos_ack::failed;
					}

					std::lock_guard lock{ mutex_ };
					groups_.erase(name);

					return proto::paxos_ack::done;
				}
				else if(content_proto::actions::insert == act)
				{
					proto::payload::endpoint endpt;

					std::size_t len = body.size() - sizeof(act);
					if(!endpt.deserialize(body.data() + sizeof(act), len))
					{
						log.err("failed to insert: failed to deserialize");
						return proto::paxos_ack::failed;
					}

					if(endpt.id.empty())
					{
						log.err("failed to insert: empty node id");
						return proto::paxos_ack::failed;
					}

					if(endpt.id != nodecaps_->identity())
					{
						std::lock_guard lock{mutex_};
						
					}

					return proto::paxos_ack::done;
				}
				else if(content_proto::actions::remove == act)
				{
					auto idsv = body.substr(sizeof(act));

					std::string id{idsv.data(), idsv.size()};

					std::lock_guard lock{ mutex_ };
					
					
					return proto::paxos_ack::done;
				}

				return proto::paxos_ack::done;
			};

			adapter.rollback = [this](std::string_view proposal){
				return proto::paxos_ack::done;
			};

			adapter.serialize = [this](std::filesystem::path p){

				auto log = nodecaps_->log("content-adapter-serialize");

				log.msg("Serializes the state to file:", p);
				
				::nlohmann::json nodes;
				
				std::lock_guard lock{ mutex_ };


				::nlohmann::json json;

				json["nodes"] = nodes;
				std::ofstream os{p};
				os<<json;
			};

			adapter.deserialize = [this](std::filesystem::path p){
				auto log = nodecaps_->log("content-reactor-deserialize");

				log.msg("Deserialize the state from file:", p);

				std::ifstream is{p};
				if(is)
				{
					::nlohmann::json json;
					is>>json;

					auto& nodes = json["nodes"];
					for(auto & node : nodes.items())
					{
						//node.key()
					}

				}

				//auto nodes = 

			};

			paxos_.start(adapter);
		}
	private:
		abs::nodecaps_interface * const nodecaps_;

		paxos paxos_;

		mutable std::recursive_mutex mutex_;
		std::map<std::string, group> groups_;	///< The group table
		std::map<std::string, node> nodes_;		///< The nodes that added to the cluster.
	};

}

#endif