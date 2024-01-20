#ifndef PIPASHAN_NET_SERVER_INCLUDED
#define PIPASHAN_NET_SERVER_INCLUDED

#include <boost/asio.hpp>
#include <thread>
#include <map>
#include <memory>
#include <atomic>
#include <type_traits>

#include "../execute.hpp"
#include "../proto.hpp"
#include "../log.hpp"

namespace pipashan::net
{
	enum class recv_status
	{
		failed,
		failed_skip,
		success,
		success_skip
	};

	enum class online_status
	{
		offline,
		online,			///< A node is connected to this node.
		online_accepted	///< This node has connected to a node 
	};

	inline const char* to_string(online_status s)
	{
		const char* texts[] = {"offline", "online", "online_accepted"};
		if(static_cast<std::size_t>(s) < 3)
			return texts[static_cast<std::size_t>(s)];

		return "invalid status";		
	}

	using buffer = proto::buffer;

	
	class nodeconn:
		public std::enable_shared_from_this<nodeconn>
	{
		struct resp_handler
		{
			proto::api api;
			std::uint32_t pktcode;
			const void * owner;

			std::function<bool(std::shared_ptr<buffer>&, std::error_code)> handler;

			bool match(const proto::packet& pkt) const noexcept
			{
				return (api == pkt.api_key && pktcode == pkt.pktcode);
			}
		};
	public:
		using tcp = boost::asio::ip::tcp;
		using pointer = std::shared_ptr<nodeconn>;
		using address = proto::payload::address;
		using endpoint = proto::payload::endpoint;

		class recv_interface
		{
		public:
			using nodeconn	= net::nodeconn;
			using buffer	= proto::buffer;
			using recv_status = net::recv_status;
			using online_status = net::online_status;

			virtual ~recv_interface() = default;

			///A new nodeconn is established.
			///The blocking procedure is not allowed in method established().
			virtual void established(nodeconn::pointer) = 0;
			virtual void online_changed(nodeconn::pointer, online_status os) = 0;

			virtual recv_status recv(nodeconn::pointer, std::shared_ptr<buffer>) = 0;
		};

		class paxos_acceptor_interface
		{
		public:
			using nodeconn	= net::nodeconn;
			using buffer	= proto::buffer;
			using recv_status = net::recv_status;
			using online_status = net::online_status;

			virtual ~paxos_acceptor_interface() = default;

			virtual void delay_catchup() = 0;

			virtual void online_changed(nodeconn::pointer, online_status os) = 0;

			virtual void paxos_propose(nodeconn::pointer, std::uint32_t pktcode, const proto::payload::paxos_propose&) = 0;
			virtual void paxos_cancel(nodeconn::pointer, const proto::payload::paxos_cancel&) = 0;
			virtual proto::paxos_ack paxos_accept(nodeconn::pointer, const proto::payload::paxos_accept&) = 0;
			virtual proto::paxos_ack paxos_rollback(const proto::payload::paxos_rollback&) = 0;
			virtual void paxos_catchup(nodeconn::pointer, const proto::payload::paxos_catchup&) = 0;
			virtual void paxos_catchup_data(nodeconn::pointer, const proto::payload::paxos_catchup_data&, std::shared_ptr<buffer>) = 0;
			virtual void paxos_catchup_accepted(nodeconn::pointer, const proto::payload::paxos_catchup_accepted&) = 0;
		};

		class paxos_proposer_interface
		{
		public:
			virtual ~paxos_proposer_interface() = default;

			virtual void online_changed(nodeconn::pointer, online_status os) = 0;
			virtual void paxos_propose_resp(nodeconn::pointer, const proto::payload::paxos_propose_resp&) = 0;
		};

		struct paxos_recv_type
		{
			paxos_acceptor_interface * acceptor{ nullptr };
			paxos_proposer_interface* proposer{ nullptr };
			std::atomic<std::size_t> use_count{ 0 };
		};

		~nodeconn()
		{
			close();
		}

		void add(recv_interface* r)
		{
			std::lock_guard lock{ mutex_ };
			if(recvset_.count(r))
				return;

			recvset_[r] = 0;
		}

		void add_paxos(const std::string& paxos, paxos_acceptor_interface* r, paxos_proposer_interface* p)
		{
			std::lock_guard lock{ mutex_ };
			if(paxos_recvset_.count(paxos))
				return;

			auto& recv = paxos_recvset_[paxos];
			recv.acceptor = r;
			recv.proposer = p;
		}

		void erase_paxos(const std::string& paxos, bool forced)
		{
			std::lock_guard lock{ mutex_ };

			if(forced)
			{
				std::lock_guard lock{ mutex_ };
				paxos_recvset_.erase(paxos);
				return;
			}

			while(true)
			{
				{
					std::lock_guard lock{ mutex_ };
					auto i = paxos_recvset_.find(paxos);
					if(i == paxos_recvset_.cend())
						return;

					if(0 == i->second.use_count)
					{
						paxos_recvset_.erase(i);
						return;
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds{80});
			}

		}

		/// Erases a recv handler from the connection.
		/**
		 * @param r The recv handler
		 * @param forced Indicates whether or not to remove a recv handler forcefully. If forced is false, it
		 * 				waits for the ref-counter to 0 to remove the recv handler. it is order to avoid a crash
		 * 				error that recv handler's destructor erases the recv handler when the recv handler is calling.
		*/
		void erase(recv_interface* r, bool forced)
		{
			if(forced)
			{
				std::lock_guard lock{ mutex_ };
				auto i = recvset_.find(r);
				if(i != recvset_.cend())
					recvset_.erase(i);

				return;
			}

			//Known issue. When a recv handler's destructor erases the recv handler while the recv handler is being called.
			//It causes a dead-locking.

			while(true)
			{
				{
					std::lock_guard lock{ mutex_ };

					auto i = recvset_.find(r);
					if(i == recvset_.cend())
						return;

					if(0 == i->second)
					{
						recvset_.erase(i);
						return;
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds{80});
			}
		}

		/// Accepts this connection for the remote node which asked to connect to this node.
		void online(const proto::payload::register_node& remote_endpt)
		{
			address_.ipv4.clear();
			address_.ipv6.clear();

			try{
				address_.ipv4 = socket_.remote_endpoint().address().to_v4().to_string();
			}
			catch(const std::exception&){}

			try{
				address_.ipv6 = socket_.remote_endpoint().address().to_v6().to_string();
			}
			catch(const std::exception&){}

			address_.port = remote_endpt.listen_port;
			id_ = remote_endpt.id;

			auto me = this->shared_from_this();
			recv_->established(me);
			recv_->online_changed(me, online_status::online);
		}

		/// This connection is accepted by the remote node.
		void online_accepted(const std::string& ident)
		{
			id_ = ident;
			auto me = this->shared_from_this();
			recv_->established(me);
			recv_->online_changed(me, online_status::online_accepted);
		}

		const std::string& id() const noexcept
		{
			return id_;
		}

		std::string short_id() const noexcept
		{
			if(id_.size() < 4)
				return id_;

			return id_.substr(0, 4);
		}

		const address& addr() const noexcept
		{
			return address_;
		}

		endpoint endpt() const noexcept
		{
			return endpoint{id_, address_};
		}

		void close()
		{
			boost::system::error_code err;
			socket_.shutdown(socket_.shutdown_both, err);
			socket_.close(err);			
		}

		void linger()
		{
			boost::asio::socket_base::linger option;
			socket_.get_option(option);
			bool is_set = option.enabled();
			unsigned short timeout = option.timeout();
		}

		static pointer create(std::shared_ptr<tcp::socket> socket, abs::nodelog_interface* nodelog, recv_interface* recv, execute* exec)
		{
			return pointer{new nodeconn{socket, nodelog, recv, exec}};
		}

		/// Starts this connection.
		void start()
		{
			_m_read();
		}

		template<typename Function>
		void async_connect(const address& addr, Function&& fn)
		{
			address_ = addr;

			boost::system::error_code err;
			
			try
			{
				socket_.async_connect(
					boost::asio::ip::tcp::endpoint{boost::asio::ip::address::from_string(addr.ipv4.c_str()), addr.port},
					std::forward<Function>(fn)
					);
			}
			catch(...)
			{
			}
		}

		std::string remote_address() const
		{
			return socket_.remote_endpoint().address().to_string();
		}

		std::uint16_t pktcode() noexcept
		{
			return pktcode_++;
		}

		bool ack(std::uint16_t pktcode, std::uint32_t value)
		{
			proto::packet pkt;

			pkt.signature = proto::signature;
			pkt.api_key = proto::api::ack;
			pkt.pktcode = pktcode;
			pkt.len = value;

			pkt.time_point = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

			auto tm = std::chrono::high_resolution_clock::now();

			std::lock_guard lock{ somutex_ };

			boost::system::error_code err;
			boost::asio::write(socket_, boost::asio::buffer(reinterpret_cast<char*>(&pkt), sizeof pkt), err);

			return !err;
		}

		template<proto::Payload Payload>
		bool send(std::uint16_t pktcode, const Payload& payload)
		{
			proto::buffer buf;

			buf.serialize(pktcode, payload);

			auto tm = std::chrono::high_resolution_clock::now();

			std::lock_guard lock{ somutex_ };

			boost::system::error_code err;
			boost::asio::write(socket_, buf.sendbuf(), err);

			return !err;
		}


		template<proto::Payload Payload>
		bool send(const Payload& payload)
		{
			proto::buffer buf;

			buf.serialize(pktcode_++, payload);

			auto tm = std::chrono::high_resolution_clock::now();

			std::lock_guard lock{ somutex_ };

			boost::system::error_code err;
			boost::asio::write(socket_, buf.sendbuf(), err);

			return !err;
		}

		template<proto::PartialPayload Payload>
		bool send(const Payload& payload, const char* data, std::size_t bytes)
		{
			proto::buffer buf;
			buf.serialize(pktcode_++, payload, bytes);

			auto tm = std::chrono::high_resolution_clock::now();

			std::lock_guard lock{ somutex_ };

			boost::system::error_code err;

			//sendbuf creates a buffer whose length is pkt->len + sizeof(packet) - bytes
			boost::asio::write(socket_, buf.sendbuf(bytes), err);
			if(!err)
				boost::asio::write(socket_, boost::asio::buffer(data, bytes), err);

			return !err;
		}

		
		bool send(const proto::buffer& buf)
		{
			auto tm = std::chrono::high_resolution_clock::now();
			std::lock_guard lock{ somutex_ };

			boost::system::error_code err;
			boost::asio::write(socket_, buf.sendbuf(), err);

			return !err;
		}

		template<proto::Payload Payload, typename RespHandler>
		bool send(const void* owner, const Payload& payload, RespHandler handler)
		{
			proto::buffer buf;
			buf.serialize(pktcode_++, payload);
			
			return send(owner, buf, std::move(handler));
		}

		template<typename RespHandler>
		bool send(const void* owner, const proto::buffer& buf, RespHandler handler)
		{
			return send(owner, buf.data(), buf.size(), std::move(handler));
		}

		template<typename RespHandler>
		bool send(const void* owner, const char* data, std::size_t len, RespHandler handler)
		{
			auto p = std::make_shared<resp_handler>();

			auto pkt = reinterpret_cast<const proto::packet*>(data);

			p->api = proto::resp(pkt->api_key);

			if constexpr (!std::is_same_v<bool, std::invoke_result_t<RespHandler, std::shared_ptr<buffer>&, std::error_code>>)
			{
				p->handler = [handler = std::move(handler)](std::shared_ptr<buffer>& buf, std::error_code err) mutable {
					handler(buf, err);
					return true;
				};
			}
			else
				p->handler = std::move(handler);

			p->pktcode = pkt->pktcode;
			p->owner = owner;

			{
				std::lock_guard lock{ mutex_ };
				handlers_.push_back(p);
			}

			std::lock_guard lock{ somutex_ };
			if (good_)
			{
				boost::asio::async_write(socket_, boost::asio::buffer(data, len), [this, p](const boost::system::error_code& err, std::size_t){
					if(err)
						_m_erase(p);
				});
				return true;
			}
			return false;
		}
	private:
		nodeconn(std::shared_ptr<tcp::socket>& s, abs::nodelog_interface* nodelog, recv_interface* recv, execute* exec):
			nodelog_(nodelog),
			socket_(std::move(*s)),
			execute_(exec),
			recv_(recv)
		{
		}

		void _m_read()
		{

			auto self = this->shared_from_this();

			boost::asio::async_read(socket_, boost::asio::buffer(reinterpret_cast<char*>(&packet_), sizeof(packet_)), [this, self](const boost::system::error_code& err, std::size_t){
				if(err || proto::signature != packet_.signature)
				{
					_m_offline(self);
					return;
				}

				if(proto::api::ack == packet_.api_key)
				{
					_m_handle_ack();

					this->_m_read();
					return;
				}

				if(packet_.len > proto::max_packet_bytes)
				{
					auto log = nodelog_->log("async-read");
					log.err("Length of packet beyonds the max packet bytes(", proto::max_packet_bytes ,"):", packet_.len, " bytes, node(", self->id(),") api_key:", to_string(packet_.api_key) );
					_m_offline(self);
					return;
				}

				if(0 == packet_.len)
				{
					this->_m_read();
					return;
				}

				auto buf = std::make_shared<buffer>(packet_);

				auto asio_buf = boost::asio::buffer(buf->payload(), buf->pkt()->len);

				boost::asio::async_read(socket_, asio_buf, [this, self, buf = std::move(buf)](const boost::system::error_code& err, std::size_t) mutable {
					if(err)
					{
						_m_offline(self);
						return;
					}

					switch(recv_->recv(self, buf))
					{
					case recv_status::failed:
						_m_handle(buf, err);
					case recv_status::failed_skip:
						_m_recv(self, buf);
						return;
					case recv_status::success:
						_m_handle(buf, err);
					case recv_status::success_skip:
						_m_recv(self, buf);
						break;
					}

					this->_m_read();
				});
			});			
		}

		void _m_offline(pointer self)
		{
			good_ = false;
			auto use_count_before = self.use_count();
			handlers_.clear();

			recv_->online_changed(self, online_status::offline);

			_m_foreach_recv([self](recv_interface* r){
				r->online_changed(self, online_status::offline);
			});
		}

		paxos_recv_type* _m_paxos_recv(const std::string& paxos)
		{
			std::lock_guard lock{ mutex_ };

			auto i = paxos_recvset_.find(paxos);
			if(i == paxos_recvset_.cend())
				return nullptr;

			++(i->second.use_count);

			return &i->second;
		}

		void _m_paxos_ack(std::uint16_t pktcode, const proto::payload::paxos_meta& meta, proto::paxos_ack ack)
		{
			proto::payload::paxos_ack paxack;
			paxack.meta.paxos = meta.paxos;
			paxack.meta.key = meta.key;
			paxack.meta.internal = meta.internal;
			paxack.ack = ack;
			this->send(pktcode, paxack);
		}

		void _m_recv_paxos(nodeconn::pointer self, const std::shared_ptr<buffer>& buf, std::int64_t time_pt)
		{
			auto log = nodelog_->log("acceptor-recv-paxos");

			proto::paxos_ack ack{};
			std::string key;
			bool internal = false;	// This value is likely false

			auto api_key = buf->pkt()->api_key;

			if(proto::api::paxos_propose == buf->pkt()->api_key)
			{
				proto::payload::paxos_propose paxpro;
				if(!buf->deserialize(paxpro))
				{
					log.err("paxos_propose - deserialization failed");
					return;
				}

				auto pr = _m_paxos_recv(paxpro.meta.paxos);
#ifdef SHOWLOG_PROPOSE
				log.msg("paxos-propose(", paxpro.meta.key.short_hex(), ") of paxos(", paxpro.meta.paxos, "): recv = ", (pr ? "yes" : "empty"));
#endif
				if(pr)
				{
					pr->acceptor->paxos_propose(self, buf->pkt()->pktcode, paxpro);
					--pr->use_count;
				}
				else
					log.err("paxos_propose not paxos(\"", paxpro.meta.paxos, "\") found");
			}
			else if (proto::api::paxos_propose_resp == buf->pkt()->api_key)
			{
				proto::payload::paxos_propose_resp paxrsp;
				if (!buf->deserialize(paxrsp))
				{
					log.err("paxos_propose_resp() deserialize error");
					return;
				}

				//log.msg("paxos_propose_resp(",paxrsp.meta.key.short_hex(),") by node(",self->id(),") received. retval=", to_string(paxrsp.ack));

				auto pr = _m_paxos_recv(paxrsp.meta.paxos);
				if (pr)
				{
					pr->proposer->paxos_propose_resp(self, paxrsp);
					--pr->use_count;
				}
			}
			else if(proto::api::paxos_cancel == buf->pkt()->api_key)
			{
				proto::payload::paxos_cancel paxcan;
				if(!buf->deserialize(paxcan))
					return;

				auto pr = _m_paxos_recv(paxcan.meta.paxos);
				if(pr)
				{
					pr->acceptor->paxos_cancel(self, paxcan);

					_m_paxos_ack(buf->pkt()->pktcode, paxcan.meta, proto::paxos_ack::done);

					--pr->use_count;
				}
			}
			else if(proto::api::paxos_accept == buf->pkt()->api_key)
			{
				proto::payload::paxos_accept paxacc;
				if (!buf->deserialize(paxacc))
				{
					log.err("undeserialized paxos_accept");
					return;
				}

				auto pr = _m_paxos_recv(paxacc.meta.paxos);

#ifdef SHOWLOG_ACCEPT
				log.msg("paxos-accept:", paxacc.meta.key.short_hex(), " paxos(", paxacc.meta.paxos,") recv =  ", pr ? "yes" : "empty");
#endif
				if(pr)
				{
					auto ack = pr->acceptor->paxos_accept(self, paxacc);
#ifdef SHOWLOG_ACCEPT
					log.msg("paxos-accept:", paxacc.meta.key.short_hex(), " ack = ", to_string(ack));
#endif
					_m_paxos_ack(buf->pkt()->pktcode, paxacc.meta, ack);
					--pr->use_count;
				}
			}
			else if(proto::api::paxos_rollback == buf->pkt()->api_key)
			{
				proto::payload::paxos_rollback paxrol;
				if(!buf->deserialize(paxrol))
					return;

				auto pr = _m_paxos_recv(paxrol.meta.paxos);
				if(pr)
				{
					auto ack = pr->acceptor->paxos_rollback(paxrol);

					_m_paxos_ack(buf->pkt()->pktcode, paxrol.meta, ack);

					--pr->use_count;
				}
			}
			else if(proto::api::paxos_catchup == buf->pkt()->api_key)
			{
				proto::payload::paxos_catchup paxcth;
				if(!buf->deserialize(paxcth))
					return;

				auto pr = _m_paxos_recv(paxcth.paxos);
				if(pr)
				{
					pr->acceptor->paxos_catchup(self, paxcth);
					--pr->use_count;
				}
			}
			else if(proto::api::paxos_catchup_data == buf->pkt()->api_key)
			{
				proto::payload::paxos_catchup_data paxcthdata;
				if(!buf->deserialize(paxcthdata))
					return;

				auto pr = _m_paxos_recv(paxcthdata.paxos);
				if(pr)
				{
					pr->acceptor->paxos_catchup_data(self, paxcthdata, buf);
					--pr->use_count;
				}
			}
			else if(proto::api::paxos_catchup_accepted == buf->pkt()->api_key)
			{
				//The remote node became ready, it tells this node to catchup now
				proto::payload::paxos_catchup_accepted paxacc;
				if(!buf->deserialize(paxacc))
					return;

				auto pr = _m_paxos_recv(paxacc.paxos);
				if(pr)
				{
					pr->acceptor->paxos_catchup_accepted(self, paxacc);
					--pr->use_count;
				}
			}
	
		}

		void _m_recv(pointer self, std::shared_ptr<buffer> buf)
		{
			if(proto::is_paxos(buf->pkt()->api_key))
			{
				auto time_pt = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();

				switch (buf->pkt()->api_key)
				{
					//Some of paxos commands must be invoked in the networking thread for execution order.
				case proto::api::paxos_propose:
				case proto::api::paxos_propose_resp:
					_m_recv_paxos(self, buf, time_pt);
					break;
				default:
					execute_->run([this, self, buf, time_pt] {
						_m_recv_paxos(self, buf, time_pt);
						});
				}
				return;
			}

			_m_foreach_recv([self, buf](recv_interface* recv){
				recv->recv(self, buf);
			});
		}

		template<typename Function>
		void _m_foreach_recv(Function fn)
		{
			std::vector<nodeconn::recv_interface*> receivers;

			std::lock_guard lock{ mutex_ };

			if(recvset_.empty())
				return;

			for(auto & rv : recvset_)
				receivers.push_back(rv.first);

			execute_->run([this, receivers, fn = std::move(fn)]{
					for(auto recv: receivers)
					{
						{
							std::lock_guard lock{ mutex_ };

							auto i = recvset_.find(recv);
							if(i == recvset_.cend())
								continue;

							//Increases the ref-counter and release the mutex
							++(i->second);
						}

						//Now it is safe to call receiver.

						fn(recv);

						//The recv handler may be removed forcefully
						//Check it before decreasing the ref-counter.
						std::lock_guard lock{ mutex_ };
						auto i = recvset_.find(recv);
						if(i != recvset_.cend())
							--(i->second);
					}
			});
		}

		void _m_handle_ack()
		{
			std::shared_ptr<resp_handler> p;
			{
				std::lock_guard lock{ mutex_ };
				for(auto i = handlers_.cbegin(); i != handlers_.cend(); ++i)
				{
					if((*i)->match(packet_))
					{
						p = *i;
						break;
					}
				}
			}

			if(p)
			{
				boost::system::error_code err;

				auto buf = std::make_shared<buffer>();
				buf->assign(packet_);
				if (p->handler(buf, err))
				{
					std::lock_guard lock{ mutex_ };
					for (auto i = handlers_.cbegin(); i != handlers_.cend(); ++i)
					{
						if ((*i)->match(packet_))
						{
							handlers_.erase(i);
							break;
						}
					}
				}
			}
		}

		void _m_handle(std::shared_ptr<buffer> buf, std::error_code err)
		{
			std::shared_ptr<resp_handler> p;
			{
				std::lock_guard lock{ mutex_ };

				for(auto i = handlers_.cbegin(); i != handlers_.cend(); ++i)
				{
					if((*i)->match(*buf->pkt()))
					{
						p = *i;
						handlers_.erase(i);
						break;
					}
				}
			}

			if (p && p->handler(buf, err))
			{
				std::lock_guard lock{ mutex_ };
				for (auto i = handlers_.cbegin(); i != handlers_.cend(); ++i)
				{
					if ((*i)->match(packet_))
					{
						handlers_.erase(i);
						break;
					}
				}
			}
		}

		void _m_erase(std::shared_ptr<resp_handler> p)
		{
			std::lock_guard lock{ mutex_ };

			auto i = std::find(cbegin(handlers_), cend(handlers_), p);
			if (i != handlers_.cend())
				handlers_.erase(i);
		}
	private:
		abs::nodelog_interface* const nodelog_;
		std::string id_;
		address address_;
		std::atomic<std::uint16_t> pktcode_{ 0 };
		bool good_{ true };

		execute* const execute_;	// The execution object is provided by manager
		recv_interface* const recv_;

		proto::packet packet_;

		mutable std::mutex somutex_;	//socket mutex
		mutable std::recursive_mutex mutex_;	
		tcp::socket socket_;

		std::map<std::string, paxos_recv_type> paxos_recvset_;
		std::map<recv_interface*, std::atomic<std::size_t>> recvset_;

		std::vector<std::shared_ptr<resp_handler>> handlers_;
	};


	template<typename TCPConnFactory>
	class tcp_server
	{
		static constexpr std::size_t concurrent_hints = 1;
	public:
		using tcp = boost::asio::ip::tcp;

		template<typename ...Args>
		tcp_server(Args&& ... args):
			factory_(std::forward<Args>(args)...)
		{
		}

		~tcp_server()
		{
			ioctx_.stop();
			for(auto & thread : threads_)
			{
				if(thread && thread->joinable())
					thread->join();
			}
		}

		boost::asio::io_context& ioctx()
		{
			return ioctx_;
		}

		void start(std::uint16_t port)
		{
			if(acceptor_)
				return;

			ioctx_work_ = std::make_unique<boost::asio::io_context::work>(ioctx_);

			acceptor_ = std::make_unique<tcp::acceptor>(ioctx_, tcp::endpoint(tcp::v4(), port));

			auto endpt = acceptor_->local_endpoint();

			listen_port_ = endpt.port();

			_m_listen();

			for(std::size_t i = 0; i < concurrent_hints; ++i)
			{
				threads_.emplace_back(std::make_shared<std::thread>([this]{
					ioctx_.run();
				}));
			}
		}

		std::uint16_t listen_port() const
		{
			return listen_port_;
		}

		TCPConnFactory& factory()
		{
			return factory_;
		}
	private:

		void _m_listen()
		{
			auto socket = std::make_shared<tcp::socket>(ioctx_);

			acceptor_->async_accept(*socket, [this, socket](const boost::system::error_code& err){
				if(!err)				
					_m_begin_socket(socket);

				_m_listen();
			});
		}

		void _m_begin_socket(std::shared_ptr<tcp::socket> socket)
		{
			auto pkt = std::make_shared<proto::packet>();

			boost::asio::async_read(*socket, boost::asio::buffer(reinterpret_cast<char*>(pkt.get()), sizeof(proto::packet)), [this,socket,pkt](const boost::system::error_code& err, std::size_t){
				if(err || proto::signature != pkt->signature || pkt->len > proto::max_packet_bytes)
					return;

				if(proto::api::register_node != pkt->api_key)
					return;

				buffer buf{*pkt};
				
				//make the boost buffer before async_read, because the buf will be moved
				auto boost_buf = buf.payload();

				boost::asio::async_read(*socket, boost_buf, [this, socket, buf = std::move(buf)](const boost::system::error_code& err, std::size_t){
					if(err)
						return;

					if(proto::api::register_node == buf.pkt()->api_key)
					{
						proto::payload::register_node payload;
						if(!buf.deserialize(payload))
							return;

						proto::payload::register_node_resp resp;

						resp.id = factory_.id();

						boost::system::error_code err;
						boost::asio::write(*socket, buffer{buf.pkt()->pktcode, resp}.sendbuf(), err);

						if(err)
							return;

						auto conn = factory_.make_nodeconn(socket);

						conn->online(payload);
						
						conn->start();
						
					}
				});
			});			
		}
	private:
		TCPConnFactory factory_;
		std::atomic<std::uint16_t> listen_port_{ 0 };
		std::unique_ptr<std::thread> thread_;

		boost::asio::io_context ioctx_{ concurrent_hints };
		std::unique_ptr<boost::asio::io_context::work> ioctx_work_;
		std::unique_ptr<tcp::acceptor> acceptor_;
		std::vector<std::shared_ptr<std::thread>> threads_;	
	};
}

#endif