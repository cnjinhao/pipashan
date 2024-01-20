#ifndef PIPASHAN_NET_FACTORY_INCLUDED
#define PIPASHAN_NET_FACTORY_INCLUDED

#include "server.hpp"

namespace pipashan::net
{
	class factory
	{
	public:
		using tcp = boost::asio::ip::tcp;

		using endpoint = proto::payload::endpoint;

		factory(abs::nodelog_interface* nodelog, nodeconn::recv_interface* recv, execute* exec, const std::string& id):
			id_(id),
			nodelog_(nodelog),
			recv_(recv),
			exec_(exec)
		{}

		const std::string& id() const noexcept
		{
			return id_;
		}

		nodeconn::pointer make_nodeconn(std::shared_ptr<tcp::socket> s)
		{
			return nodeconn::create(s, nodelog_, recv_, exec_);
		}
	private:
		std::string const id_;
		abs::nodelog_interface* const nodelog_;
		nodeconn::recv_interface* const recv_;
		execute* const exec_;
	};
}

#endif