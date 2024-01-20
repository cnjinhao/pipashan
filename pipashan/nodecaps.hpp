#ifndef PIPASHAN_NODECAPS_INCLUDED
#define PIPASHAN_NODECAPS_INCLUDED

#include "net/server.hpp"
#include "log.hpp"
#include <set>

namespace pipashan
{
	inline std::string short_id(const std::string& id)
	{
		if(id.size() < 4)
			return id;

		return id.substr(0, 4);
	}

	namespace abs
	{
		class nodecaps_interface
		{
		public:
			using address = proto::payload::address;
			using nodeconn = net::nodeconn;

			virtual ~nodecaps_interface() = default;
			virtual const std::string& identity() const noexcept = 0;

			virtual pipashan::log log(const std::string& title) = 0;
			virtual pipashan::log log(const std::string& paxos, const std::string& title) = 0;
			virtual abs::nodelog_interface* nodelog() = 0;

			///Returns the absolute path of the working path
			virtual const std::filesystem::path& working_path() const noexcept = 0;
			virtual proto::payload::endpoint endpoint() const = 0;
			virtual void make_nodeconn(const address&, std::function<bool(nodeconn::pointer, bool)>) = 0;
			virtual std::set<nodeconn::pointer> find(const std::string& id) = 0;
			virtual void add_recv(nodeconn::recv_interface*) = 0;
			virtual void erase_recv(nodeconn::recv_interface*) = 0;
			virtual execute& exec() noexcept = 0;

			virtual void delete_paxos(const std::string&) = 0;
		};
	}
}

#endif