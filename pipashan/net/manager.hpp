#ifndef PIPASHAN_NET_MANAGER_INCLUDED
#define PIPASHAN_NET_MANAGER_INCLUDED

#include "factory.hpp"
#include "../nodecaps.hpp"
#include <set>
#include <map>

#include "../c++defines.hpp"

namespace pipashan::net
{
	class manager:
		public nodeconn::recv_interface
	{
	public:
		using address = proto::payload::address;
		using endpoint = proto::payload::endpoint;

		struct target_node
		{
			address addr;
			std::vector<std::function<bool(nodeconn::pointer, bool)>> reactors;
		};

		manager(const std::string& id, std::vector<address> cluster, std::uint16_t listen_port, abs::nodecaps_interface* nodecaps):
			nodecaps_(nodecaps),
			cluster_(std::move(cluster)),
			tcpserv_(nodecaps->nodelog(), this, &execute_, id)
		{
			tcpserv_.start(listen_port);

			nodecaps->log("net::manager").msg("listening on port ", tcpserv_.listen_port());
		}

		void start()
		{
			_m_try_connect_to_cluster(0);
		}

		/// Returns the actual listening port
		std::uint16_t listen_port() const noexcept
		{
			return tcpserv_.listen_port();
		}

		/// Adds an object for receiving network packet
		void add_recv(nodeconn::recv_interface* recv)
		{
			std::lock_guard lock{ recv_.mutex };

			if(recv_.holders.count(recv) == 0)
				recv_.holders[recv] = 0;
		}

		void erase_recv(nodeconn::recv_interface* recv)
		{
			while(true)
			{
				{
					std::lock_guard lock{ recv_.mutex };

					auto i = recv_.holders.find(recv);
					if(i == recv_.holders.cend())
						return;

					if(i->second == 0)
					{
						recv_.holders.erase(i);
						return;
					}
				}

				std::this_thread::sleep_for(std::chrono::milliseconds{100});
			}
		}
		
		void make_nodeconn(const address& addr, std::function<bool(nodeconn::pointer, bool)> complete_fn)
		{
			if(addr.empty())
			{
				if(complete_fn)
					complete_fn(nullptr, false);
				return;
			}
			std::lock_guard lock{ mutex_ };
			
			for(auto & target : remote_targets_)
			{
				if(addr == target.addr)
				{
					//The addr is connecting now.
					if(complete_fn)
						target.reactors.emplace_back(std::move(complete_fn));
					return;
				}
			}

			auto & target = remote_targets_.emplace_back();
			target.addr = addr;

			if(complete_fn)
				target.reactors.emplace_back(std::move(complete_fn));

			_m_make_nodeconn(addr);
		}

		std::set<nodeconn::pointer> find_nodeconn(const std::string& id)
		{
			std::lock_guard lock{ mutex_ };

			auto i = table_.find(id);
			if(i == table_.end())
				return {};

			return i->second;
		}

		execute& exec() noexcept
		{
			return execute_;
		}

	private:
		nodeconn::pointer _m_find(const address& addr) const
		{
			for(auto& m : table_)
			{
				if(m.second.empty())
					continue;

				auto xaddr = m.second.cbegin()->get()->addr();
				if((!xaddr.ipv4.empty()) && (xaddr.ipv4 == addr.ipv4))
					return *m.second.cbegin();

				if((!xaddr.ipv6.empty()) && (xaddr.ipv6 == addr.ipv6))
					return *m.second.cbegin();	
			}
			return nullptr;
		}

		void _m_make_nodeconn(const address& addr)
		{
			_m_make_nodeconn(addr, [this, addr](nodeconn::pointer conn) {

				std::lock_guard lock{ mutex_ };

				auto i = std::find_if(remote_targets_.begin(), remote_targets_.end(), [addr](const auto& tar) {
					return (tar.addr == addr);
					});

				if (i == remote_targets_.end())
					return;

				if (conn)
				{
					for (auto& fn : i->reactors)
						fn(conn, true);

					remote_targets_.erase(i);
					return;
				}

				//failed to connect to the remote node.
				//Determines whether not to retry.

				for (auto u = i->reactors.begin(); u != i->reactors.end();)
				{
					if (!(*u)(nullptr, false))
					{
						u = i->reactors.erase(u);
					}
					else
						++u;
				}

				if (i->reactors.empty())
				{
					remote_targets_.erase(i);
					return;
				}

				execute_.run([this, addr] {
					std::this_thread::sleep_for(std::chrono::seconds{ 2 });
						_m_make_nodeconn(addr);
					});
				});
		}

		void _m_make_nodeconn(const address& addr, std::function<void(nodeconn::pointer)> complete_fn)
		{
			auto so = std::make_shared<boost::asio::ip::tcp::socket>(tcpserv_.ioctx());
			
			auto conn = nodeconn::create(so, nodecaps_->nodelog(), this, &execute_);
			if(conn)
			{
				conn->async_connect(addr, [this, conn, complete_fn, addr](const boost::system::error_code& err){
					if(err)
					{
						complete_fn(nullptr);
						return;
					}

					proto::payload::register_node payload;
					payload.id = tcpserv_.factory().id();
					payload.listen_port = tcpserv_.listen_port();
#ifdef PIPASHAN_OS_WINDOWS
					payload.pid = ::GetCurrentProcessId();
#else
					payload.pid = ::getpid();
#endif
					conn->start();

					conn->send(this, {0, payload}, [this, addr, conn, complete_fn](std::shared_ptr<buffer> buf, std::error_code err){
						if(err)
						{
							complete_fn(nullptr);
							return;
						}

						proto::payload::register_node_resp resp;
						if(!buf->deserialize(resp))
						{
							conn->close();

							complete_fn(nullptr);

							return;
						}

						conn->online_accepted(resp.id);

						complete_fn(conn);
					});
				});
			}
		}
	private:
		void _m_try_connect_to_cluster(std::size_t index)
		{
			if(cluster_.empty())
				return;

			if(index >= cluster_.size())
			{
				index = 0;
				std::this_thread::sleep_for(std::chrono::seconds{5});
			}
			
			this->_m_make_nodeconn(cluster_[index], [this, index](nodeconn::pointer conn){

				auto log = nodecaps_->log("network");
				if(nullptr == conn)
				{
					log.err("failed to connect to cluster:", cluster_[index]);

					execute_.run([this, index]{
						_m_try_connect_to_cluster(index + 1);
					});

					return;
				}

				log.msg("connected to cluster:", cluster_[index]);
			});
		}

		template<typename Function>
		void _m_foreach_recv(Function fn)
		{
			std::vector<nodeconn::recv_interface*> receivers;

			std::lock_guard lock{ recv_.mutex };

			if(recv_.holders.empty())
				return;

			for(auto & rv : recv_.holders)
				receivers.push_back(rv.first);

			execute_.run([this, receivers, fn = std::move(fn)]{
					for(auto recv: receivers)
					{
						{
							std::lock_guard lock{ recv_.mutex };

							auto i = recv_.holders.find(recv);
							if(i == recv_.holders.cend())
								continue;

							//Increases the ref-counter and release the mutex
							++(i->second);
						}

						//Now it is safe to call receiver.

						fn(recv);

						//Decreases the ref-counter
						std::lock_guard lock{ recv_.mutex };
						--recv_.holders[recv];
					}
			});
		}
	private:
		virtual void established(nodeconn::pointer conn) override
		{
			{
				std::lock_guard lock{ mutex_ };

				auto& nodeset = table_[conn->id()];

				nodeset.insert(conn);
			}

			std::lock_guard lock{ recv_.mutex };

			for (auto& recv : recv_.holders)
				recv.first->established(conn);
		}
		
		virtual void online_changed(nodeconn::pointer conn, online_status os) override
		{
			{
				std::lock_guard lock{ mutex_ };

				auto& nodeset = table_[conn->id()];

				if(online_status::offline == os)
				{
					auto nodeid = conn->id();
					nodeset.erase(conn);

					if (nodeset.empty())
					{
						table_.erase(nodeid);
					}
				}
			}

			_m_foreach_recv([conn, os](nodeconn::recv_interface* recv){
				recv->online_changed(conn, os);
			});
		}

		virtual recv_status recv(nodeconn::pointer conn, std::shared_ptr<buffer> buf) override
		{
			_m_foreach_recv([conn, buf](nodeconn::recv_interface* recv){
				recv->recv(conn, buf);
			});

			return recv_status::success;
		}

	private:
		abs::nodecaps_interface* const	nodecaps_;
		execute						execute_;
		std::vector<address>		cluster_;
		tcp_server<factory>			tcpserv_;

		mutable std::recursive_mutex	mutex_;

		std::map<std::string, std::set<nodeconn::pointer>> table_;

		struct recv_container
		{
			mutable std::recursive_mutex mutex;
			std::map<nodeconn::recv_interface*, std::atomic<std::size_t>> holders;
		}recv_;

		std::list<target_node> remote_targets_;	///< The container holds the endpoints which are being connected now.

	};
}

#endif