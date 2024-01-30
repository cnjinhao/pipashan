#ifndef PIPASHAN_PROTO_INCLUDED
#define PIPASHAN_PROTO_INCLUDED

#include <map>
#include <string>
#include <vector>
#include <optional>
#include "serialization.hpp"
#include "identifier.hpp"

namespace pipashan::proto
{
	constexpr std::size_t max_packet_bytes = 64 * 1024;

	using file_size_t = std::uint64_t;

	enum class api: std::uint8_t
	{
		ack,
		register_node,
		register_node_resp,
		content_nodes,
		content_create_group,
		content_delete_group,

		paxos_create,
		paxos_pause,
		paxos_ready,	///< the status of a remote node
		paxos_notify_ready,	///< Notify other nodes a node is ready.
		paxos_propose,
		paxos_propose_resp,
		paxos_cancel,
		paxos_accept,
		paxos_rollback,
		paxos_ack,
		paxos_catchup,
		paxos_catchup_data,
		paxos_catchup_accepted,	///< Tells the delayed catchup remote nodes this node is ready and catchup is allowed.

		paxos_exists,	///< Determines if the requester node is the paxos node of a specified paxos.
						///< It responds an ack. Value of ack.len is
						///< 	0: The requester node is a node of the specified paxos nodes.
						///<	1: The requester node is not a node of the specified paxos nodes.
						///<	2: Pending value, the remote node can't determine. The remote node is not a paxos node or it's not ready,
		paxos_endpoint,	///< Request the endpoints of the paxos cluster
		paxos_endpoint_resp,
		paxos_cursors,
		paxos_cursors_resp,
	};

	enum class paxos_ack: std::uint8_t
	{
		ready,
		conflicted,
		failed,
		done
	};

	inline std::string to_string(api key)
	{
		switch (key)
		{
		case api::paxos_propose:
			return "paxos_propose";
		case api::paxos_accept:
			return "paxos_accept";
		default:
			break;
		}

		auto s = std::to_string(static_cast<int>(key));
		return "other(" + s + ")";
	}

	inline std::string to_string(paxos_ack ack)
	{
		const char* texts[] = { "ready", "conflicted", "failed", "done" };

		if (static_cast<std::uint8_t>(ack) < 4)
			return texts[static_cast<std::uint8_t>(ack)];

		return "unkonwn";
	}

	enum class paxos_internal: std::uint8_t
	{
		insert,
		remove
	};

	inline api resp(api api_key)
	{
		switch(api_key)
		{
		case api::register_node: return api::register_node_resp;
		case api::paxos_catchup: return api::paxos_catchup_data;
		case api::paxos_catchup_data: return api::paxos_catchup;

		case api::paxos_propose:	return api::paxos_propose_resp;
		case api::paxos_endpoint:	return api::paxos_endpoint_resp;
		case api::paxos_cursors:	return api::paxos_cursors_resp;
		case api::paxos_accept:
		case api::paxos_cancel:
		case api::paxos_rollback:
			return api::paxos_ack;
		default:
			break;
		}
		return api::ack;
	}

	inline bool is_paxos(api api_key)
	{
		return (api::paxos_create <= api_key && api_key <= api::paxos_exists);
	}

	inline std::size_t as_size_t(const char* p)
	{
		return static_cast<std::size_t>(*reinterpret_cast<const std::uint8_t*>(p));
	}

	constexpr std::uint16_t signature = 0xABCD;	//KL

	struct packet
	{
		std::uint16_t	signature;
		api				api_key;
		std::uint16_t	pktcode;
		std::uint32_t	len;
		//Debug
		std::uint64_t	time_point;
	};

	template<typename T>
	concept Payload = requires(T t)
	{
		{ T::api_key }	-> std::convertible_to<api>;
		{ T::fixed }	-> std::convertible_to<bool>;

		//t.serialize((char*){});
		//{ t.deserialize((char*){}, std::size_t{}) } -> std::convertible_to<bool>;
	};

	template<typename T>
	concept PartialPayload = requires(T t)
	{
		{ T::api_key }	-> std::convertible_to<api>;
		{ T::fixed }	-> std::convertible_to<bool>;

		{ t.partial_bytes() } -> std::convertible_to<std::size_t>;
		{t.partial_serialize((char*)nullptr)} -> std::convertible_to<char*>;
		{ t.deserialize((char*)nullptr, std::size_t{}) } -> std::convertible_to<const char*>;
	};

	template<typename T>
	concept Deserialable = requires(T t)
	{
		{ t.deserialize((const char*)nullptr, std::size_t{}) } -> std::convertible_to<const char*>;
	};

	namespace payload
	{
		inline char* put_idstr(char* data, const std::string& str)
		{
			if(str.size() > 255)
				throw std::invalid_argument("Length of Paxos ID or node ID is larger than 255");

			*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(str.size());
			std::memcpy(data + 1, str.data(), str.size());
			return data + str.size() + 1;
		}

		inline const char* get_idstr(const char* data, std::size_t len, std::string& s)
		{
			if(len < 1) return nullptr;

			auto size = as_size_t(data);

			if(len < 1 + size) return nullptr;

			s.assign(data + 1, size);

			return data + 1 + size;
		}

		struct address
		{
			std::string ipv4;
			std::string ipv6;
			std::uint16_t port;

			address() = default;

			address(std::string ipv4, std::string ipv6, std::uint16_t port):
				ipv4(std::move(ipv4)),
				ipv6(std::move(ipv6)),
				port(port)
			{
			}

			bool operator==(const address& addr) const
			{
				return (ipv4 == addr.ipv4 && ipv6 == addr.ipv6 && port == addr.port);
			}

			bool empty() const noexcept
			{
				return ipv4.empty() && ipv6.empty();
			}
		};

		struct endpoint
		{
			std::string id;
			address	addr;

			bool operator==(const endpoint& endpt) const
			{
				return (id == endpt.id && addr == endpt.addr);
			}

			std::size_t bytes() const noexcept
			{
				return 5 + id.size() + addr.ipv4.size() + addr.ipv6.size();
			}

			char* serialize(char* data) const
			{
				data = put_idstr(data, id);

				data = put_idstr(data, addr.ipv4);
				data = put_idstr(data, addr.ipv6);

				*reinterpret_cast<std::uint16_t*>(data) = addr.port;

				return data + 2;
			}

			const char* deserialize(const char* data, std::size_t& size)
			{
				auto len = size;
				if(len < 5) return nullptr;

				data = get_idstr(data, len, id);
				if(nullptr == data)
					return nullptr;

				len -= 1 + id.size();

				data = get_idstr(data, len, addr.ipv4);
				if(nullptr == data)
					return nullptr;

				len -= 1 + addr.ipv4.size();

				data = get_idstr(data, len, addr.ipv6);
				if(nullptr == data)
					return nullptr;
				
				len -= 1 + addr.ipv6.size();

				if(len < 2)
					return nullptr;

				addr.port = *reinterpret_cast<const std::uint16_t*>(data);

				size = len - 2;
				return data + 2;
			}
		};


		template<api APIKey, bool Fixed = false>
		struct packet_payload_meta
		{
			static constexpr api api_key = APIKey;
			static constexpr bool fixed = Fixed;
		};


		struct register_node: packet_payload_meta<api::register_node>
		{
			std::uint32_t	pid;
			std::uint16_t	listen_port;
			std::string		id;

			std::size_t bytes() const noexcept
			{
				return 7 + id.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint32_t*>(data) = pid;
				*reinterpret_cast<std::uint16_t*>(data + 4) = listen_port;

				return put_idstr(data + 6, id);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 7) return nullptr;

				pid = *reinterpret_cast<const std::uint32_t*>(data);
				listen_port = *reinterpret_cast<const std::uint16_t*>(data + 4);
				return get_idstr(data + 6, len - 6, id);
			}
		};

		struct register_node_resp: packet_payload_meta<api::register_node_resp>
		{
			std::string id;	///< Identity of the connected node.

			std::size_t bytes() const noexcept
			{
				return 1 + id.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(id.size());
				std::memcpy(data + 1, id.data(), id.size());

				return data + 1 + id.size();
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 1) return nullptr;

				auto const idsz = as_size_t(data);

				if(len < 1 + idsz) return nullptr;

				id.assign(data + 1, idsz);

				return data + 1 + idsz;
			}
		};

		struct content_nodes: packet_payload_meta<api::content_nodes>
		{
			std::vector<endpoint> nodes;

			std::size_t bytes() const noexcept
			{
				std::size_t len = 2;
				for(auto & endpt: nodes)
					len += endpt.bytes();

				return len;
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint16_t*>(data) = static_cast<std::uint16_t>(nodes.size());
				data += 2;
				for(auto & endpt: nodes)
					data = endpt.serialize(data);

				return data;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 2) return nullptr;
				auto count = *reinterpret_cast<const std::uint16_t*>(data);

				len -= 2;
				data += 2;

				nodes.clear();
				for(std::size_t i = 0; i < count; ++i)
				{
					data = nodes.emplace_back().deserialize(data, len);
					if(nullptr == data)
						return nullptr;
				}
				return data;
			}
		};

		struct content_create_group: packet_payload_meta<api::content_create_group>
		{
			std::string name;

			std::size_t bytes() const noexcept
			{
				return 1 + name.size();
			}

			char* serialize(char* data) const noexcept
			{
				if(name.size() > 255)
					return nullptr;

				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(name.size());

				std::memcpy(data + 1, name.data(), name.size());

				return data + 1 + name.size();
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 1) return nullptr;

				auto sz = as_size_t(data);

				if(len < 1 + sz)
					return nullptr;

				name.assign(data + 1, sz);
				return data + 1 + sz;
			}
		};

		struct content_delete_group: packet_payload_meta<api::content_delete_group>
		{
			std::string name;

			std::size_t bytes() const noexcept
			{
				return 1 + name.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(name.size());
				std::memcpy(data + 1, name.data(), name.size());

				return data + 1 + name.size();
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 1) return nullptr;

				auto sz = as_size_t(data);
				if(len < sz + 1)
					return nullptr;

				name.assign(data + 1, sz);
				return data + 1 + sz;
			}
		};

		struct paxos_meta
		{
			std::string paxos;
			identifier key;
			bool internal;
			std::int64_t time;
			std::uint16_t time_seq;

			std::size_t bytes() const noexcept
			{
				return 12 + paxos.size() + key.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(paxos.size());
				std::memcpy(data + 1, paxos.data(), paxos.size());

				std::memcpy(data + 1 + paxos.size(), key.data(), key.size());

				data += 1 + paxos.size() + key.size();

				*reinterpret_cast<bool*>(data) = internal;
				*reinterpret_cast<std::int64_t*>(data + 1) = time;
				*reinterpret_cast<std::uint16_t*>(data + 9) = time_seq;

				return data + 11;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				constexpr auto fixed_size = 12 + identifier::size();

				if(len < fixed_size) return nullptr;

				auto paxos_len = *reinterpret_cast<const std::uint8_t*>(data);

				if(len < fixed_size + paxos_len) return nullptr;

				paxos.assign(data + 1, paxos_len);

				key.assign(data + 1 + paxos_len);

				data += 1 + paxos_len + key.size();

				internal = *reinterpret_cast<const bool*>(data);
				time	 = *reinterpret_cast<const std::int64_t*>(data + 1);
				time_seq = *reinterpret_cast<const std::uint16_t*>(data + 9);

				return data + 11;
			}
		};


		struct paxos_create: packet_payload_meta<api::paxos_create>
		{
			std::string paxos;
			std::uint8_t working_node_size;

			std::size_t bytes() const noexcept
			{
				return 2 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				data = put_idstr(data, paxos);

				*reinterpret_cast<std::uint8_t*>(data) = working_node_size;

				return data + 1;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 2) return nullptr;

				data = get_idstr(data, len, paxos);
				if(nullptr == data)
					return nullptr;

				if(len < 2 + paxos.size())
					return nullptr;

				working_node_size = *reinterpret_cast<const std::uint8_t*>(data);
				return data + 1;
			}
		};

		struct paxos_pause: packet_payload_meta<api::paxos_pause>
		{
			std::string paxos;
			bool paused;

			std::size_t bytes() const noexcept
			{
				return 2 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(paxos.size());
				std::memcpy(data + 1, paxos.data(), paxos.size());

				data = put_idstr(data, paxos);
				if(nullptr == data)
					return nullptr;

				*reinterpret_cast<bool*>(data) = paused;
				return data + 1;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 2) return nullptr;

				data = get_idstr(data, len, paxos);
				if(nullptr == data)
					return nullptr;

				if(len < 2 + paxos.size())
					return nullptr;

				this->paused = *reinterpret_cast<const bool*>(data);

				return data + 1;
			}		
		};

		struct paxos_ready: packet_payload_meta<api::paxos_ready>
		{
			std::string paxos;
			std::uint8_t action;	///< 0: Query, 1: Make it ready, 2: Make it ready by electing. 

			std::size_t bytes() const noexcept
			{
				return 2 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(paxos.size());
				std::memcpy(data + 1, paxos.data(), paxos.size());

				data = put_idstr(data, paxos);
				if(nullptr == data)
					return nullptr;

				*reinterpret_cast<std::uint8_t*>(data) = action;
				return data + 1;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 2) return nullptr;

				data = get_idstr(data, len, paxos);
				if(nullptr == data)
					return nullptr;

				if(len < 2 + paxos.size())
					return nullptr;

				action = *reinterpret_cast<const std::uint8_t*>(data);

				return data + 1;
			}
		};

		struct paxos_notify_ready: packet_payload_meta<api::paxos_notify_ready>
		{
			std::string paxos;
			std::string node;

			std::size_t bytes() const
			{
				return 2 + paxos.size() + node.size();
			}

			char* serialize(char* data) const
			{
				data = put_idstr(data, paxos);
				return put_idstr(data, node);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 2) return nullptr;
				data = get_idstr(data, len, paxos);
				if(nullptr == data || len < 2 + paxos.size())
					return nullptr;

				return get_idstr(data,  len - 1 - paxos.size(), node);
			}
		};

		struct paxos_propose: packet_payload_meta<api::paxos_propose>
		{
			paxos_meta meta;
			std::string body;

			std::size_t bytes() const
			{
				return meta.bytes() + 4 + body.size();
			}

			void serialize(char* data) const noexcept
			{
				data = meta.serialize(data);
				*reinterpret_cast<std::uint32_t*>(data) = static_cast<std::uint32_t>(body.size());

				std::memcpy(data + 4, body.data(), body.size());
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				auto next = meta.deserialize(data, len);
				if(nullptr == next)
					return nullptr;

				len -= next - data;
				data = next;

				if(len < 4)
					return nullptr;

				auto body_len = *reinterpret_cast<const std::uint32_t*>(data);
				if(body_len + 4 > len)
					return nullptr;

				body.assign(data + 4, body_len);
				return data + 4 + body_len;
			}

		};

		struct paxos_propose_resp : packet_payload_meta<api::paxos_propose_resp>
		{
			paxos_meta meta;
			proto::paxos_ack ack;
			std::optional<identifier> first;

			std::size_t bytes() const noexcept
			{
				return meta.bytes() + 2 + (first ? 16 : 0);
			}

			void serialize(char* data) const noexcept
			{
				auto p = meta.serialize(data);

				*reinterpret_cast<proto::paxos_ack*>(p) = ack;

				if(first)
				{
					auto & fkey = first.value();
					*reinterpret_cast<std::uint8_t*>(p + 1) = fkey.size();
					std::memcpy(p + 2, first.value().data(), fkey.size());
				}
				else
					*reinterpret_cast<std::uint8_t*>(p + 1) = 0;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				auto next = meta.deserialize(data, len);
				if (nullptr == next)
					return nullptr;

				len -= next - data;

				if (len > 1)
				{
					ack = *reinterpret_cast<const proto::paxos_ack*>(next);

					std::uint8_t fkey_sz = *reinterpret_cast<const std::uint8_t*>(next + 1);
					if(16 == fkey_sz)
					{
						identifier fkey;
						fkey.assign(reinterpret_cast<const char*>(next + 2));
						first = fkey;
					}
					else
					{
						fkey_sz = 0;
						first.reset();
					}

					return next + 2 + fkey_sz;
				}

				return nullptr;
			}
		};

		template<api APIKey>
		struct basic_paxos_meta: packet_payload_meta<APIKey>
		{
			paxos_meta meta;

			std::size_t bytes() const noexcept
			{
				return meta.bytes();
			}

			void serialize(char* data) const noexcept
			{
				meta.serialize(data);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				return meta.deserialize(data, len);
			}
		};

		using paxos_cancel = basic_paxos_meta<api::paxos_cancel>;
		using paxos_accept = basic_paxos_meta<api::paxos_accept>;
		using paxos_rollback = basic_paxos_meta<api::paxos_rollback>;

		struct paxos_ack: packet_payload_meta<api::paxos_ack>
		{
			paxos_meta meta;
			proto::paxos_ack ack;

			std::size_t bytes() const noexcept
			{
				return meta.bytes() + 1;
			}

			void serialize(char* data) const noexcept
			{
				*reinterpret_cast<proto::paxos_ack*>(meta.serialize(data)) = ack;

			}

			const char* deserialize(const char* data, std::size_t len)
			{
				auto next = meta.deserialize(data, len);
				if(nullptr == next)
					return nullptr;

				len -= next - data;

				if(len)
				{
					ack = *reinterpret_cast<const proto::paxos_ack*>(next);
					return next + 1;
				}

				return nullptr;
			}
		};

		struct paxos_cursor
		{
			std::string node;
			identifier key;
			std::int64_t time;
			std::uint16_t time_seq;

			std::size_t bytes() const noexcept
			{
				return 11 + node.size() + key.size();
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(node.size());
				std::memcpy(data + 1, node.data(), node.size());

				std::memcpy(data + 1 + node.size(), key.data(), key.size());

				data += 1 + node.size() + key.size();

				*reinterpret_cast<std::uint64_t*>(data) = time;
				*reinterpret_cast<std::uint16_t*>(data + 8) = time_seq;

				return data + 10;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				constexpr auto fixed_size = 11 + identifier::size();

				if(len < fixed_size) return nullptr;
				auto node_len = *reinterpret_cast<const std::uint8_t*>(data);

				if(len < fixed_size + node_len) return nullptr;
				node.assign(data + 1, node_len);

				if(len < fixed_size + node_len) return nullptr;
				key.assign(data + 1 + node_len);

				data += 1 + node_len + key.size();

				time = *reinterpret_cast<const std::uint64_t*>(data);
				time_seq = *reinterpret_cast<const std::uint16_t*>(data + 8);

				return data + 10;
			}
		};

		struct paxos_catchup: packet_payload_meta<api::paxos_catchup>
		{
			std::string paxos;
			bool		done;		///< tells the remote node the last catchup data is received.
			std::uint64_t internal_off;
			std::uint64_t external_off;
			std::uint64_t anchors_off;
			std::vector<paxos_cursor> cursors;

			std::size_t bytes() const noexcept
			{
				std::size_t len = 30 + paxos.size();
				for(auto& tc : cursors)
					len += tc.bytes();

				return len;
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(paxos.size());
				std::memcpy(data + 1, paxos.data(), paxos.size());

				data += 1 + paxos.size();

				*reinterpret_cast<bool*>(data) = done;
				*reinterpret_cast<std::uint64_t*>(data + 1) = internal_off;
				*reinterpret_cast<std::uint64_t*>(data + 9) = external_off;
				*reinterpret_cast<std::uint64_t*>(data + 17) = anchors_off;



				*reinterpret_cast<std::uint32_t*>(data + 25) = static_cast<std::uint32_t>(cursors.size());
				data += 29;

				for(auto & tc : cursors)
					data = tc.serialize(data);

				return data;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 30) return nullptr;

				auto paxos_len = as_size_t(data);

				if(len < 30 + paxos_len) return nullptr;
				paxos.assign(data + 1, paxos_len);

				done = *reinterpret_cast<const bool*>(data + 1 + paxos_len);
				internal_off = *reinterpret_cast<const std::uint64_t*>(data + 2 + paxos_len);
				external_off = *reinterpret_cast<const std::uint64_t*>(data + 10 + paxos_len);
				anchors_off = *reinterpret_cast<const std::uint64_t*>(data + 18 + paxos_len);

				auto const count = *reinterpret_cast<const std::uint32_t*>(data + 26 + paxos_len);

				data += 30 + paxos_len;
				len -= 30 + paxos_len;

				for(std::size_t i = 0; i < count; ++i)
				{
					auto next = cursors.emplace_back().deserialize(data, len);
					if((nullptr == next) && (static_cast<std::size_t>(next - data) > len))
						return nullptr;

					len -= next - data;
					data = next;
				}

				return data;
			}

		};

		enum class paxos_catchup_data_status: std::uint8_t
		{
			normal,
			refused,
			last
		};

		struct paxos_catchup_data: packet_payload_meta<api::paxos_catchup_data>
		{
			std::string paxos;
			paxos_catchup_data_status status;
			std::string_view bufview;

			std::size_t partial_bytes() const noexcept
			{
				return 6 + paxos.size();
			}

			char* partial_serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(paxos.size());
				std::memcpy(data + 1, paxos.data(), paxos.size());

				*reinterpret_cast<paxos_catchup_data_status*>(data + 1 + paxos.size()) = status;

				*reinterpret_cast<std::uint32_t*>(data + 2 + paxos.size()) = static_cast<std::uint32_t>(bufview.size());
				return data + 6 + paxos.size();
			}

			std::size_t bytes() const noexcept
			{
				return 6 + paxos.size() + bufview.size();
			}

			char * serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint8_t*>(data) = static_cast<std::uint8_t>(paxos.size());
				std::memcpy(data + 1, paxos.data(), paxos.size());

				*reinterpret_cast<paxos_catchup_data_status*>(data + 1 + paxos.size()) = status;

				*reinterpret_cast<std::uint32_t*>(data + 2 + paxos.size()) = static_cast<std::uint32_t>(bufview.size());

				if(!bufview.empty())
					std::memcpy(data + 6 + paxos.size(), bufview.data(), bufview.size());

				return data + 6 + paxos.size() + bufview.size();
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 6) return nullptr;

				auto paxos_sz = as_size_t(data);
				if(len < 6 + paxos_sz) return nullptr;

				paxos.assign(data + 1, paxos_sz);

				status = *reinterpret_cast<const paxos_catchup_data_status*>(data + 1 + paxos_sz);

				auto length = *reinterpret_cast<const std::uint32_t*>(data + 2 + paxos_sz);

				if(len < 6 + paxos_sz + length) return nullptr;

				bufview = std::string_view{data + 6 + paxos_sz, length};
				return data + 6 + paxos_sz + length;
			}
		};

		struct paxos_catchup_accepted: packet_payload_meta<api::paxos_catchup_accepted>
		{
			std::string paxos;

			std::size_t bytes() const noexcept
			{
				return 1 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				return put_idstr(data, paxos);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				return get_idstr(data, len, paxos);
			}
		};

		struct paxos_exists: packet_payload_meta<api::paxos_exists>
		{
			std::string paxos;

			std::size_t bytes() const noexcept
			{
				return 1 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				return put_idstr(data, paxos);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				return get_idstr(data, len, paxos);
			}
		};

		struct paxos_endpoint: packet_payload_meta<api::paxos_endpoint>
		{
			std::string paxos;

			std::size_t bytes() const noexcept
			{
				return 1 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				return put_idstr(data, paxos);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				return get_idstr(data, len, paxos);
			}
		};

		struct paxos_endpoint_resp: packet_payload_meta<api::paxos_endpoint_resp>
		{
			std::vector<endpoint> nodes;

			std::size_t bytes() const noexcept
			{
				std::size_t len = 2;
				for(auto & endpt: nodes)
					len += endpt.bytes();

				return len;
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint16_t*>(data) = static_cast<std::uint16_t>(nodes.size());
				data += 2;
				for(auto & endpt: nodes)
					data = endpt.serialize(data);

				return data;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				if(len < 2) return nullptr;
				auto count = *reinterpret_cast<const std::uint16_t*>(data);

				len -= 2;
				data += 2;

				nodes.clear();
				for(std::size_t i = 0; i < count; ++i)
				{
					data = nodes.emplace_back().deserialize(data, len);
					if(nullptr == data)
						return nullptr;
				}
				return data;
			}
		};

		struct paxos_cursors: packet_payload_meta<api::paxos_cursors>
		{
			std::string paxos;

			std::size_t bytes() const noexcept
			{
				return 1 + paxos.size();
			}

			char* serialize(char* data) const noexcept
			{
				return put_idstr(data, paxos);
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				return get_idstr(data, len, paxos);
			}
		};

		struct paxos_cursors_resp: packet_payload_meta<api::paxos_cursors_resp>
		{
			std::map<std::string, paxos_meta> cursors;

			std::size_t bytes() const noexcept
			{
				std::size_t len = 2;
				for(auto & cur : cursors)
				{
					len += 1 + cur.first.size() + cur.second.bytes();
				}
				return len;
			}

			char* serialize(char* data) const noexcept
			{
				*reinterpret_cast<std::uint16_t*>(data) = static_cast<std::uint16_t>(cursors.size());
				data += 2;

				for(auto & cur : cursors)
				{
					data = put_idstr(data, cur.first);
					data = cur.second.serialize(data);
				}
				return data;
			}

			const char* deserialize(const char* data, std::size_t len)
			{
				const std::size_t size = *reinterpret_cast<const std::uint16_t*>(data);

				data += 2;
				len -= 2;

				for(std::size_t i = 0; i < size; ++i)
				{
					std::string node;
					data = get_idstr(data, len, node);
					if(data == nullptr)
						return nullptr;

					len -= node.size() + 1;

					auto next = cursors[node].deserialize(data, len);

					if(nullptr == next)
						return nullptr;

					len -= (next - data);
					data = next;
				}
				return data;
			}
		};

		using paxos_internal_insert = endpoint;
		using paxos_internal_remove = endpoint;
	}// end namespace payload


	class buffer
	{
		buffer(const buffer&) = delete;
		buffer& operator=(const buffer&) = delete;
	public:
		buffer() = default;

		buffer(buffer&& rhs):
			len_(rhs.len_),
			data_(rhs.data_)
		{
			rhs.len_ = 0;
			rhs.data_ = nullptr;
		}

		buffer& operator=(buffer&& rhs)
		{
			if(this != &rhs)
			{
				delete [] data_;
				len_ = rhs.len_;
				data_ = rhs.data_;

				rhs.len_ = 0;
				rhs.data_ = nullptr;
			}
			return *this;
		}

		buffer(const proto::packet& pkt)
		{
			auto const space = sizeof(packet) + pkt.len;
			_m_alloc(space);
			std::memcpy(data_, &pkt, sizeof(pkt));			
		}

		template<Payload T>
		buffer(std::uint16_t pktcode, const T& payload)
		{
			serialize(pktcode, payload);
		}

		~buffer()
		{
			delete [] data_;
		}

		const char* data() const
		{
			return data_;
		}

		char* data() = delete;

		std::size_t size() const
		{
			if((len_ >= sizeof(packet)) && data_)
			{
				auto pkt = reinterpret_cast<const packet*>(data_);

				if(pkt->len + sizeof(packet) <= len_)
					return pkt->len + sizeof(packet);
			}

			return 0;
		}

		boost::asio::mutable_buffer payload()
		{
			return boost::asio::mutable_buffer{data_ + sizeof(packet), reinterpret_cast<const packet*>(data_)->len};
		}

		boost::asio::const_buffer sendbuf(std::size_t extra_bytes = 0) const
		{
			return boost::asio::const_buffer{data_, pkt()->len + sizeof(packet) - extra_bytes};
		}

		packet* pkt()
		{
			return reinterpret_cast<packet*>(data_);
		}

		const packet* pkt() const
		{
			return reinterpret_cast<const packet*>(data_);
		}

		void assign(const packet& pkt)
		{
			auto const space = sizeof(packet) + pkt.len;
			_m_alloc(space);
			std::memcpy(data_, &pkt, sizeof(pkt));
		}

		template<Payload T>
		void serialize(std::uint16_t pktcode, const T& payload)
		{
			auto space = sizeof(packet);
			
			if constexpr(!T::fixed)
				space += payload.bytes();

			_m_alloc(space);

			auto p = reinterpret_cast<packet*>(data_);
			p->signature = proto::signature;
			p->api_key = T::api_key;
			p->len = static_cast<std::uint32_t>(space - sizeof(packet));
			p->pktcode = pktcode;

			p->time_point = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

			if constexpr(T::fixed)
				std::memcpy(data_ + sizeof(packet), &payload, sizeof(payload));
			else
				payload.serialize(data_ + sizeof(packet));
		}

		template<PartialPayload T>
		void serialize(std::uint16_t pktcode, const T& payload, std::size_t attachment_bytes)
		{
			auto space = sizeof(packet);
			
			if constexpr(!T::fixed)
				space += payload.partial_bytes();

			_m_alloc(space);

			auto p = reinterpret_cast<packet*>(data_);
			p->signature = proto::signature;
			p->api_key = T::api_key;
			p->len = static_cast<std::uint32_t>(space - sizeof(packet) + attachment_bytes);
			p->pktcode = pktcode;
			p->time_point = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

			if constexpr(T::fixed)
				std::memcpy(data_ + sizeof(packet), &payload, sizeof(payload));
			else
				payload.partial_serialize(data_ + sizeof(packet));
		}

#if 1
		//template<typename Payload>
		template<Deserialable T>
		bool deserialize(T& payload) const
		{
			if((len_ >= sizeof(packet)) && data_)
			{
				auto pkt = reinterpret_cast<packet*>(data_);
				if(pkt->len + sizeof(packet) > len_)
					return false;

				return payload.deserialize(data_ + sizeof(packet), pkt->len);
			}
			return false;
		}
#else
		template<Deserialable ...T>
		bool deserialize(T& ...payloads) const
		{
			if((len_ >= sizeof(packet)) && data_)
			{
				auto pkt = reinterpret_cast<packet*>(data_);
				if(pkt->len + sizeof(packet) > len_)
					return false;

				return _m_deserialize(0, payloads...);
			}

			return false;
		}
#endif
	private:
		template<Deserialable T, Deserialable ...Payloads>
		bool _m_deserialize(std::size_t off, T& payload, Payloads& ...payloads) const
		{
				if constexpr(T::fixed)
				{
					std::memcpy(&payload, data_ + sizeof(packet) + off, sizeof(payload));
					return true;
				}
				else
				{
					auto pkt = reinterpret_cast<packet*>(data_);
					if(payload.deserialize(data_ + sizeof(packet) + off, pkt->len - off))
						return _m_deserialize(off + payload.bytes(), payloads...);
				}

				return false;
		}

		template<Deserialable T, Deserialable ...Payloads>
		bool _m_deserialize(std::size_t off, T& payload) const
		{
			if constexpr(T::fixed)
			{
				std::memcpy(&payload, data_ + sizeof(packet) + off, sizeof(payload));
				return true;
			}
			else
			{
				auto pkt = reinterpret_cast<packet*>(data_);
				return (payload.deserialize(data_ + sizeof(packet) + off, pkt->len - off));
			}
		}
	private:
		void _m_alloc(std::size_t size)
		{
			if(size > len_)
			{
				delete [] data_;
				data_ = new char[size];
				len_ = size;
			}
		}

	private:
		std::size_t len_{ 0 };
		char* data_{ nullptr };
	};



}

#endif