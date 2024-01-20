/**
 *   ____ _ ____ ____ _______ __ __________ 
 *  |  _ |_|  _ |___ | ___| |_| |___ |   | |  a Decentralized Consensus Library for Moderm C++
 * 	|  __| |  __| __ |___ |  _  | __ | | | |  version 0.1.0
 * 	|_|  |_|_|  |____|____|_| |_|____|_|___|  https://github.com/cnjinhao/pipashan
 *         
 * Licensed under the MIT License <http://opensource.org/licenses/MIT>.
 * Copyright (c) 2024, Jinhao <cnjinhao at hotmail dot com>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#ifndef PIPASHAN_NODE_INCLUDED
#define PIPASHAN_NODE_INCLUDED

#include <random>
#include <mutex>
#include <condition_variable>
#include "net/manager.hpp"
#include "modules/content.hpp"

#if defined(__APPLE__) || defined(__linux__)
#	include <arpa/inet.h>
#	include <ifaddrs.h>
#	include <net/if.h>
#	include <sys/types.h>
#else
#	include <winsock2.h>
#	include <iphlpapi.h>
#	include <ws2ipdef.h>
#	pragma comment(lib, "iphlpapi.lib")
#endif

namespace pipashan
{
	class paxos_factory_interface
	{
	public:
		virtual ~paxos_factory_interface() = default;
		virtual bool make(paxos&) noexcept = 0;
		virtual void destroy() noexcept = 0;
	};

	class node:
		public abs::nodecaps_interface,
		public abs::nodelog_interface
	{
		using paxos_type = pipashan::paxos;

		class handler: public net::nodeconn::recv_interface
		{
		public:
			handler(node& self):
				self_(self)
			{}
		private:
			virtual void established(nodeconn::pointer) override
			{
			}

			virtual void online_changed(nodeconn::pointer conn, online_status os) override
			{
				if(online_status::online == os)
				{
					//When a node is connected to this node, sends the content nodes
					self_._m_send_content_nodes(conn);
				}
			}

			virtual recv_status recv(nodeconn::pointer conn, std::shared_ptr<buffer> buf) override
			{
				if(proto::api::content_nodes == buf->pkt()->api_key)
				{
					proto::payload::content_nodes cnodes;
					if(buf->deserialize(cnodes))
						self_._m_assign_content_nodes(cnodes.nodes);
				}
				else if(proto::api::content_create_group == buf->pkt()->api_key)
				{
					proto::payload::content_create_group ccg;
					if(buf->deserialize(ccg))
					{
						create_group_status status = create_group_status::failed;

						std::lock_guard lock{self_.mutex_};
						if(self_.content_)
							status = self_.content_->create_group(ccg.name);

						conn->ack(buf->pkt()->pktcode, static_cast<std::uint8_t>(status));
					}
				}
				else if(proto::api::content_delete_group == buf->pkt()->api_key)
				{
					proto::payload::content_delete_group cdg;
					if(buf->deserialize(cdg))
					{
						bool status = false;

						std::lock_guard lock{ self_.mutex_ };
						if(self_.content_)
							status = self_.content_->delete_group(cdg.name);

						conn->ack(buf->pkt()->pktcode, status ? 0: 1);
					}
				}
				else if(proto::api::paxos_create == buf->pkt()->api_key)
					return self_._m_paxos_create(conn, buf);
				else if(proto::api::paxos_ready == buf->pkt()->api_key)
					return self_._m_paxos_ready(conn, buf);
				else if(proto::api::paxos_notify_ready == buf->pkt()->api_key)
					return self_._m_paxos_notify_ready(conn, buf);
				else if(proto::api::paxos_exists == buf->pkt()->api_key)
					return self_._m_paxos_exists(conn, buf);
				else if(proto::api::paxos_cursors == buf->pkt()->api_key)
					return self_._m_paxos_cursors(conn, buf);
				else if(proto::api::paxos_endpoint == buf->pkt()->api_key)
					return self_._m_paxos_endpoint(conn, buf);
				else if(proto::api::paxos_pause == buf->pkt()->api_key)
					return self_._m_paxos_pause(conn, buf);
				
				return recv_status::success;
			}
		private:
			node & self_;
		};

		using recv_status = net::nodeconn::recv_interface::recv_status;
		using buffer = net::nodeconn::recv_interface::buffer;

		struct paxos_runtime
		{
			struct mutex_cond
			{
				bool created{ false };
				std::mutex mutex;
				std::condition_variable cond;
			};

			std::shared_ptr<paxos_factory_interface> factory;
			std::shared_ptr<paxos_type> instance;

			std::shared_ptr<mutex_cond> waiting_signal;
		};
	public:
		using address = proto::payload::address;

		node(const std::string& id, const std::vector<address>& cluster, std::uint16_t listen_port, const std::filesystem::path& working_path):
			identity_(id),
			working_path_(_m_verify_working_path(working_path)),
			handler_(*this),
			logio_(working_path),
			manager_(id, cluster, listen_port, this)
		{
			auto endpt = this->endpoint();

			pipashan::log msg{"node", logio_};
			msg.msg("This endpoint: ", endpt.addr);

			//log::msg msg{"node"};
			//msg<<"this endpoint: ipv4="<<endpt.addr.ipv4<<" ipv6="<<endpt.addr.ipv6<<" port="<<endpt.addr.port<<std::endl;

			manager_.add_recv(&handler_);

			std::error_code err;
			if(std::filesystem::is_directory(std::filesystem::path{"paxos"} / "content" , err))
			{
				//Try to load the content on disk.
				_m_make_content(0, nullptr);
			}

			//
			if(cluster.empty())
			{
				_m_make_content(3, nullptr);
			}
		}

		void start()
		{
			manager_.start();
		}

		bool is_content_node() const
		{
			std::lock_guard lock{ mutex_ };
			return static_cast<bool>(content_);
		}

		/// Installs a paxos factory
		void paxos_factory(const std::string& paxos, std::shared_ptr<paxos_factory_interface> factory)
		{
			std::lock_guard lock{ mutex_ };
			paxgens_[paxos].factory = factory;
		}

		/// Creates a paxos with specified name
		/**
		 * @param paxos The name of paxos
		 * @param working_node_size The number of nodes that make consensus among these nodes.
		 * @return A shared pointer to the paxos instance if successfully. If it is failed to create, the specified
		 * 			name is already existing or the paxos factory is not existing, it returns nullptr.
		*/
		std::shared_ptr<paxos_type> create_paxos(const std::string& paxos, std::size_t working_node_size)
		{
			return _m_create_paxos(paxos, working_node_size, nullptr);
		}

		/// Gets the instance of paxos with specified name
		std::shared_ptr<paxos_type> paxos(const std::string& name) const
		{
			std::lock_guard lock{ mutex_ };
			auto i = paxgens_.find(name);
			if(i != paxgens_.cend())
				return i->second.instance;

			return nullptr;
		}

		template<typename Rep, typename Period>
		std::shared_ptr<paxos_type> paxos_for(const std::string& name, const std::chrono::duration<Rep, Period>& rel_time)
		{
			std::shared_ptr<paxos_runtime::mutex_cond> ws;
			{
				std::lock_guard lock{ mutex_ };
				auto i = paxgens_.find(name);
				if(i == paxgens_.cend())
					return nullptr;
				
				if(i->second.instance)
					return i->second.instance;

				if(!i->second.waiting_signal)
				{
					ws = std::make_shared<paxos_runtime::mutex_cond>();
					i->second.waiting_signal = ws;
				}
				else
					ws = i->second.waiting_signal;
			}

			{
				std::unique_lock lock{ ws->mutex };

				if(!ws->created)
					ws->cond.wait_for(lock, rel_time);
			}
			
			std::lock_guard lock{ mutex_ };

			auto i = paxgens_.find(name);

			//Literally, the iterator must exist
			if (i == paxgens_.cend())
				return nullptr;
				
			return i->second.instance;
		}


		create_group_status create_group(const std::string& name)
		{
			{
				std::lock_guard lock{ mutex_ };
				if(content_ && content_->ready())
					return content_->create_group(name);
			}

			auto conn = _m_content_node();

			auto status = create_group_status::failed;

			if(conn)
			{
				proto::payload::content_create_group ccg;
				ccg.name = name;

				std::mutex mutex;
				std::condition_variable condvar;

				std::unique_lock lock{mutex};
				conn->send(this, ccg, [this, &mutex, &condvar, &status](std::shared_ptr<buffer> buf, std::error_code err){
					if(buf && !err && (proto::api::ack == buf->pkt()->api_key))
						status = static_cast<create_group_status>(buf->pkt()->len);

					std::lock_guard lock{ mutex };
					condvar.notify_one();
				});
				condvar.wait(lock);
			}

			return status;
		}

		bool delete_group(const std::string& name)
		{
			{
				std::lock_guard lock{ mutex_ };
				if(content_ && content_->ready())
					return content_->delete_group(name);
			}

			auto conn = _m_content_node();

			if(conn)
			{
				bool status = false;
				proto::payload::content_delete_group cdg;
				cdg.name = name;

				std::mutex mutex;
				std::condition_variable condvar;

				std::unique_lock lock{mutex};
				conn->send(this, cdg, [this, &mutex, &condvar, &status](std::shared_ptr<buffer> buf, std::error_code err){

					pipashan::log msg{"delete-group", logio_};
					if(buf && !err && (proto::api::ack == buf->pkt()->api_key))
					{
						status = (0 == buf->pkt()->len);
						msg.msg("status returned by remote node = ", status);
					}
					else
						msg.msg("returned exceptional");

					std::lock_guard lock{ mutex };
					condvar.notify_one();
				});
				condvar.wait(lock);

				return status;
			}

			return false;

		}
	public:
		//Implement the nodecaps
		virtual const std::string& identity() const noexcept override
		{
			return identity_;
		}

		virtual pipashan::log log(const std::string& title) override
		{
			pipashan::log nodelog{title, logio_};
			return nodelog;
		}

		virtual pipashan::log log(const std::string& paxos, const std::string& title) override
		{
			pipashan::log nodelog{paxos, identity_, title, logio_};
			return nodelog;
		}

		virtual abs::nodelog_interface* nodelog() override
		{
			return this;
		}

		virtual const std::filesystem::path& working_path() const noexcept override
		{
			return working_path_;
		}

		virtual proto::payload::endpoint endpoint() const override
		{
			proto::payload::endpoint endpt;
			endpt.id = identity_;
			endpt.addr.port = manager_.listen_port();

#ifdef PIPASHAN_OS_WINDOWS
			proto::payload::address loopback;

			ULONG addresses_bytes = 0;

			constexpr int gaa_flags = GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_INCLUDE_GATEWAYS;

			::GetAdaptersAddresses(AF_UNSPEC, gaa_flags, nullptr, nullptr, &addresses_bytes);

			auto const addresses_buf = reinterpret_cast<IP_ADAPTER_ADDRESSES*>(operator new(addresses_bytes));

			if (NO_ERROR == ::GetAdaptersAddresses(AF_UNSPEC, gaa_flags, nullptr, addresses_buf, &addresses_bytes))
			{
				constexpr DWORD text_size = 100;
				char text[text_size];

				auto addr = addresses_buf;
				while (addr)
				{
					if ((IfOperStatusUp == addr->OperStatus) && addr->FirstGatewayAddress)
					{
						auto ucaddr = addr->FirstUnicastAddress;

						while (ucaddr)
						{
							if (AF_INET == ucaddr->Address.lpSockaddr->sa_family)
							{
								auto sain = reinterpret_cast<sockaddr_in*>(ucaddr->Address.lpSockaddr);

								if (IF_TYPE_SOFTWARE_LOOPBACK == addr->IfType)
									loopback.ipv4.assign(inet_ntop(AF_INET, &sain->sin_addr, text, text_size));
								else
									endpt.addr.ipv4.assign(inet_ntop(AF_INET, &sain->sin_addr, text, text_size));

							}
							else if(AF_INET6 == ucaddr->Address.lpSockaddr->sa_family)
							{
								auto sain = reinterpret_cast<sockaddr_in6*>(ucaddr->Address.lpSockaddr);

								if (IF_TYPE_SOFTWARE_LOOPBACK == addr->IfType)
									loopback.ipv6.assign(inet_ntop(AF_INET6, &sain->sin6_addr, text, text_size));
								else
									endpt.addr.ipv6.assign(inet_ntop(AF_INET6, &sain->sin6_addr, text, text_size));
							}

							ucaddr = ucaddr->Next;
						}
					}

					addr = addr->Next;
				}
			}

			operator delete(addresses_buf);


			if (endpt.addr.ipv4.empty() && endpt.addr.ipv6.empty())
				endpt.addr.ipv4 = "127.0.0.1";
#else
			std::vector<boost::asio::ip::address> loopbacks;
			std::vector<boost::asio::ip::address> res;

			ifaddrs *ifs;
			if (!::getifaddrs(&ifs)) {

				for (auto addr = ifs; addr != nullptr; addr = addr->ifa_next) {
					// No address? Skip.
					if (addr->ifa_addr == nullptr) continue;

					// Interface isn't active? Skip.
					if (!(addr->ifa_flags & IFF_UP)) continue;

					if(!_m_ifa_name_predicate(addr->ifa_name))
						continue;

					bool loopback = addr->ifa_flags & IFF_LOOPBACK;
					bool running = addr->ifa_flags & IFF_RUNNING;

					if(addr->ifa_addr->sa_family == AF_INET) {
						auto val = boost::asio::ip::make_address_v4(ntohl(reinterpret_cast<sockaddr_in *>(addr->ifa_addr)->sin_addr.s_addr));
						
						if(IFF_LOOPBACK & addr->ifa_flags)
							loopbacks.push_back(val);
						else
							res.push_back(val);
					} else if(addr->ifa_addr->sa_family == AF_INET6) {

						auto saddr = reinterpret_cast<sockaddr_in6 *>(addr->ifa_addr);

						boost::asio::ip::address_v6::bytes_type buf;
						std::memcpy(buf.data(), saddr->sin6_addr.s6_addr, sizeof(saddr->sin6_addr));
						auto val = boost::asio::ip::make_address_v6(buf, saddr->sin6_scope_id);

						if(IFF_LOOPBACK & addr->ifa_flags)
							loopbacks.push_back(val);
						else
							res.push_back(val);

					} else continue;
				}
				::freeifaddrs(ifs);
			}

			for (auto& addr : res)
			{
				if (addr.is_v4())
					endpt.addr.ipv4 = addr.to_string();
				else if (addr.is_v6())
					endpt.addr.ipv6 = addr.to_string();
			}

			if (endpt.addr.ipv4.empty() && endpt.addr.ipv6.empty())
				for (auto& addr : loopbacks)
				{
					if (addr.is_v4())
						endpt.addr.ipv4 = addr.to_string();
					else if (addr.is_v6())
						endpt.addr.ipv6 = addr.to_string();
				}
#endif
			return endpt;
		}

		/// Connects to the remote node.
		/**
		 * @param addr The address of remote node
		 * @param complete_fn The function is called when connect() returns. The return value of complete_fn
		 * 					determines whether nor not to retry to connect the remote node if it is failed to connect the remote node.
		*/
		virtual void make_nodeconn(const address& addr, std::function<bool(nodeconn::pointer, bool)> complete_fn) override
		{
			manager_.make_nodeconn(addr, complete_fn);
		}

		virtual std::set<nodeconn::pointer> find(const std::string& id) override
		{
			return manager_.find_nodeconn(id);
		}

		virtual void add_recv(nodeconn::recv_interface* p) override
		{
			manager_.add_recv(p);
		}

		virtual void erase_recv(nodeconn::recv_interface* p) override
		{
			manager_.erase_recv(p);
		}

		virtual execute& exec() noexcept override
		{
			return manager_.exec();
		}

		virtual void delete_paxos(const std::string& paxos) override
		{
			if(paxos == "content")
			{
				manager_.exec().run([this]{
					content_.reset();
				});
				return;
			}

			std::lock_guard lock{ mutex_ };
			auto i = paxgens_.find(paxos);

			if(i == paxgens_.cend())
				return;

			if(i->second.instance)
			{
				i->second.factory->destroy();
				i->second.instance.reset();
			}
		}
	private:
		static bool _m_ifa_name_predicate(const char* name)
		{
#ifdef PIPASHAN_OS_APPLE
			auto n = std::strlen(name);
			return (n == 3);
#else
			return true;
#endif
		}

		std::shared_ptr<paxos_type> _m_create_paxos(const std::string& paxos_id, std::size_t working_node_size, nodeconn::pointer inviter)
		{
			pipashan::log msg{"node-create_paxos", logio_};

			std::lock_guard lock{ mutex_ };

			auto i = paxgens_.find(paxos_id);
			if(i == paxgens_.cend())
			{
				msg.msg("<warning> No paxos(\"",paxos_id,"\") is defined");
				return {};
			}

			if(!i->second.instance)
			{
				i->second.instance = std::make_shared<pipashan::paxos>(paxos_id, working_node_size, this, inviter);

				if(i->second.factory->make(*i->second.instance))
				{
					if(i->second.waiting_signal)
					{
						auto wsig = i->second.waiting_signal;

						i->second.waiting_signal.reset();

						std::lock_guard lock{ wsig->mutex };
						wsig->created = true;
						wsig->cond.notify_all();
					}
					
					return i->second.instance;
				}
				else
					i->second.instance.reset();
			}

			return {};
		}

		static std::filesystem::path _m_verify_working_path(const std::filesystem::path& p)
		{
			std::error_code err;

			if(p.empty())
			{
				auto process_working_path = std::filesystem::current_path(err);
				if(!err)
					return process_working_path;
			}
			else
			{
				if(is_directory(p, err))
					return absolute(p);

				if(create_directories(p, err))
					return absolute(p);
			}

			std::string what = "working path(";
			
			what += p.string();
			what += ") is invalid";
			
			throw std::invalid_argument(what);
		}

		void _m_make_content(std::size_t working_node_size, nodeconn::pointer conn)
		{
			std::lock_guard lock{ mutex_ };
			if(content_)
				return;

			content_.reset(new content{this, working_node_size, conn});
			content_->signals.on_ready = [this]{
				//Clear the copy of content nodes when the content is ready.
				std::lock_guard lock{ mutex_ };
				content_nodes_.clear();
			};
		}
	private:
		static std::size_t _m_random_gen(std::size_t last)
		{
			if(0 == last)
				return 0;

			// Seed with a real random value, if available
			std::random_device r;

			// Choose a random mean between 1 and 6
			std::default_random_engine e1(r());
			std::uniform_int_distribution<int> uniform_dist{0, static_cast<int>(last)};
			return static_cast<std::size_t>(uniform_dist(e1));
		}

		/// Randomly returns a connection which is connected to the content node.
		nodeconn::pointer _m_content_node()
		{
			proto::payload::endpoint endpt;

			{
				std::lock_guard lock{ mutex_ };

				if(content_nodes_.empty() || content_)
					return nullptr;

				//Randomly choose a connection.
				std::vector<nodeconn::pointer> candidates;

				for(auto & node: content_nodes_)
				{
					auto conns = this->find(node.id);
					if(!conns.empty())
						candidates.push_back(*conns.cbegin());
				}

				if(!candidates.empty())
					return candidates[_m_random_gen(candidates.size() - 1)];

				//No connection available, try to create one.
				endpt = content_nodes_[_m_random_gen(content_nodes_.size() - 1)];
			}

			std::mutex mutex;
			std::condition_variable condvar;
			nodeconn::pointer content_conn;

			std::unique_lock lock{ mutex };
			this->make_nodeconn(endpt.addr, [this, &mutex, &condvar, &content_conn](nodeconn::pointer conn, bool){
				if(conn)
					content_conn = conn;

				std::lock_guard lock{ mutex };
				condvar.notify_one();

				//Don't to reconnect if it fails
				return false;
			});

			condvar.wait(lock);

			return content_conn;
		}

		void _m_send_content_nodes(nodeconn::pointer conn)
		{
			proto::payload::content_nodes cnodes;

			std::lock_guard lock{ mutex_ };
			if(content_ && content_->ready())
				cnodes.nodes = content_->nodes();
			else
				cnodes.nodes = content_nodes_;

			pipashan::log msg{"node-send_content_nodes", logio_};
			msg.msg("nodeconn = 0x", conn.get());

			conn->send(cnodes);
		}

		void _m_assign_content_nodes(const std::vector<proto::payload::endpoint>& cnodes)
		{
			std::lock_guard lock{ mutex_ };

			if(!content_)
			{
				content_nodes_ = cnodes;
				std::string logtext = "update content nodes: {";
				for(auto & node: cnodes)
				{
					logtext += short_id(node.id);
					logtext += ' ';
				}
				logtext += '}';

				pipashan::log msg{"node", logio_};
				msg.msg(logtext);

				return;
			}

			//This node has the content, the remote content nodes received may not be same with the local content nodes.
			//Because when this node is just connecting to the cluster, the content may not be synchronized completely.
			if(!content_->ready())
			{
				//temporarily holds the copy of content nodes, it will be cleared when the content is ready.
				content_nodes_ = cnodes;
			}
			else
				content_nodes_.clear();
		}

		recv_status _m_paxos_create(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_create paxcrt;
			if(!buf->deserialize(paxcrt))
			{
				conn->ack(buf->pkt()->pktcode, 1);
				return recv_status::failed;
			}

			std::uint32_t ack = 0;
			
			if(paxcrt.paxos == "content")
			{
				conn->ack(buf->pkt()->pktcode, 0);
				_m_make_content(paxcrt.working_node_size, conn);
			}
			else
			{
				if(_m_create_paxos(paxcrt.paxos, paxcrt.working_node_size, conn))
					conn->ack(buf->pkt()->pktcode, 0);
				else
					conn->ack(buf->pkt()->pktcode, 1);
			}

			return recv_status::success_skip;
		}

		/// Returns a pointer to the paxos with specified name. The mutex_ must be acquired when calling this method.
		pipashan::paxos* _m_paxos(const std::string& name) const
		{
			if(("content" == name) && content_)
				return &content_->paxos_object();

			auto i = paxgens_.find(name);
			if((i != paxgens_.cend()) && i->second.instance)
				return i->second.instance.get();

			return nullptr;
		}

		/// Query status of the node
		/**
		 * ack status in read mode: 0 = ready; 1 = not ready; 2 = paxos not running; 3 = no paxos; 4 = deser error
		 */
		recv_status _m_paxos_ready(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_ready paxrdy;
			if(!buf->deserialize(paxrdy))
			{
				conn->ack(buf->pkt()->pktcode, 4);
				return recv_status::failed;
			}

			std::uint16_t ack = 3;

			if("content" == paxrdy.paxos)
			{
				if(content_)
				{
					if(0 != paxrdy.action)
					{
						//It's not a querying
						content_->make_ready(paxrdy.action == 2);
						ack = 0;
					}
					else
						ack = content_->ready() ? 0 : 1;
				}
				else
					ack = 2;
			}
			else
			{
				std::lock_guard lock{ mutex_ };
				auto i = paxgens_.find(paxrdy.paxos);
				if(i != paxgens_.cend())
				{
					if(i->second.instance)
					{
						if(0 != paxrdy.action)
						{
							//It's not a querying
							i->second.instance->make_ready(2 == paxrdy.action);
							ack = 0;
						}
						else
							ack = i->second.instance->ready() ? 0 : 1;
					}
					else
						ack = 2;
				}
				
			}

			conn->ack(buf->pkt()->pktcode, ack);

			return recv_status::success_skip;
		}

		recv_status _m_paxos_notify_ready(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_notify_ready pax;
			if(!buf->deserialize(pax))
			{
				conn->ack(buf->pkt()->pktcode, 10);
				return recv_status::failed;
			}

			std::uint16_t ack = 0;
			if("content" == pax.paxos)
			{
				if(content_)
				{
					int retval = content_->paxos_object().notify_ready(pax.node);
					ack = static_cast<std::uint16_t>(retval);
				}
				else
					ack = 11;
			}
			else
			{
				std::lock_guard lock{ mutex_ };
				auto i = paxgens_.find(pax.paxos);
				if(i != paxgens_.cend())
				{
					if(i->second.instance)
					{
						int retval = i->second.instance->notify_ready(pax.node);
						ack = static_cast<std::uint16_t>(retval);
					}
					else
						ack = 13;
				}
				else
					ack = 12;				
			}

			conn->ack(buf->pkt()->pktcode, ack);
			return recv_status::success_skip;	
		}

		recv_status _m_paxos_exists(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_exists paxexs;
			if(!buf->deserialize(paxexs))
			{
				conn->ack(buf->pkt()->pktcode, 1 << 15);
				return recv_status::failed;
			}

			std::uint16_t ack = 0;

			if("content" == paxexs.paxos)
			{
				if(content_)
				{
					ack = (content_->ready() ? 3 : 1);
					if(content_->paxos_object().exists(conn->id()))
						ack |= (1 << 2);
				}
			}
			else
			{
				std::lock_guard lock{ mutex_ };
				auto i = paxgens_.find(paxexs.paxos);
				if(i != paxgens_.cend())
				{
					if(i->second.instance)
					{
						ack = (i->second.instance->ready() ? 3 : 1);
						if(i->second.instance->exists(conn->id()))
							ack |= (1 << 2);
					}
				}
			}

			conn->ack(buf->pkt()->pktcode, ack);

			return recv_status::success_skip;	
		}

		recv_status _m_paxos_cursors(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_cursors_resp resp;
			proto::payload::paxos_cursors paxcur;
			if(!buf->deserialize(paxcur))
			{
				conn->send(buf->pkt()->pktcode, resp);
				return recv_status::failed;
			}

			if("content" == paxcur.paxos)
			{
				if(content_)
					resp.cursors = content_->paxos_object().cursors();
			}
			else
			{
				std::lock_guard lock{ mutex_ };
				auto i = paxgens_.find(paxcur.paxos);
				if(i != paxgens_.cend())
				{
					if(i->second.instance)
						resp.cursors = i->second.instance->cursors();
				}
			}

			conn->send(buf->pkt()->pktcode, resp);

			return recv_status::success_skip;
		}

		recv_status _m_paxos_endpoint(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_endpoint_resp resp;

			proto::payload::paxos_endpoint paxept;
			if(!buf->deserialize(paxept))
			{
				conn->send(buf->pkt()->pktcode, resp);
				return recv_status::failed;
			}

			if("content" == paxept.paxos)
			{
				if(content_)
					resp.nodes = content_->paxos_object().nodes();
			}
			else
			{
				std::lock_guard lock{ mutex_ };
				auto i = paxgens_.find(paxept.paxos);
				if(i != paxgens_.cend())
				{
					if(i->second.instance)
						resp.nodes = i->second.instance->nodes();
				}
			}
			
			conn->send(buf->pkt()->pktcode, resp);

			return recv_status::success_skip;			
		}

		recv_status _m_paxos_pause(nodeconn::pointer conn, const std::shared_ptr<buffer>& buf)
		{
			proto::payload::paxos_pause paxpas;
			if(!buf->deserialize(paxpas))
			{
				conn->ack(buf->pkt()->pktcode, 1);
				return recv_status::failed;
			}

			std::uint16_t ack = 2;

			{
				std::lock_guard lock{ mutex_ };
				auto p = _m_paxos(paxpas.paxos);
				if(p)
				{
					ack = 0;
					p->recv_if()->recv_paxos_pause(paxpas.paused, conn->id());
				}
			}

			conn->ack(buf->pkt()->pktcode, ack);
			return recv_status::success_skip;
		}
	private:
		std::string const identity_;
		std::filesystem::path const working_path_;	///< The absolute path of working path
		log_details::logio logio_;

		handler			handler_;
		net::manager	manager_;

		mutable std::recursive_mutex mutex_;	
		std::unique_ptr<content> content_;
		std::vector<proto::payload::endpoint> content_nodes_;

		std::map<std::string, paxos_runtime> paxgens_;
	};
}

#endif