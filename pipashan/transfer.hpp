#ifndef PIPASHAN_TRANSFER_INCLUDED
#define PIPASHAN_TRANSFER_INCLUDED

#include <atomic>
#include <mutex>
#include <condition_variable>
#include "net/server.hpp"

namespace pipashan
{
	template<typename Container>
	class basic_transfer
	{
		class waitable: public std::enable_shared_from_this<waitable>
		{
		public:
			waitable(std::size_t s, abs::nodelog_interface* nodelog):
				resp_size_(s),
				nodelog_(nodelog)
			{}

			void resp()
			{
				++resp_;

				std::lock_guard lock{ mutex_ };
				if(resp_ >= resp_size_)
					condvar_.notify_one();
			}

			template<typename Duration>
			bool wait_for(std::size_t failed_size, Duration d, const std::string& label)
			{
				std::unique_lock lock{ mutex_ };
				if(resp_ + failed_size < resp_size_)
				{
					auto status = condvar_.wait_for(lock, d);
					if(status == std::cv_status::timeout)
					{
						auto log = nodelog_->log("transfer-wait_for");
						log.msg("timeout label = ", label);
					}
				}

				return (resp_ + failed_size >= resp_size_);
			}

		private:
			std::size_t const resp_size_;
			abs::nodelog_interface* const nodelog_;
			mutable std::mutex mutex_;
			std::condition_variable condvar_;
			std::atomic<std::size_t> resp_{ 0 };	
		};
	public:
		using container = std::remove_cvref_t<Container>;
		using buffer = proto::buffer;
		using conn_pointer = typename container::value_type::element_type::pointer;

		basic_transfer(const Container& workers, abs::nodelog_interface* nodelog):
			waitable_(std::make_shared<waitable>(workers.size(), nodelog)),
			workers_(workers)
		{
		}

		bool empty() const
		{
			return workers_.empty();
		}

		void reset(Container& workers)
		{
			workers_.swap(workers);
		}

		template<typename Payload, typename Handler>
		std::size_t send(const Payload& payload, Handler&& handler, std::string label = {})
		{
			if(workers_.empty())
				return 0;

			proto::buffer buf{0, payload};
			return send(buf, std::forward<Handler>(handler), label);
		}

		template<typename Handler>
		std::size_t send(buffer& buf, Handler&& handler, std::string label = {})
		{
			if(workers_.empty())
				return 0;

			auto w = waitable_;
			auto fn = [w, handler = std::forward<Handler>(handler)](const conn_pointer& conn, std::shared_ptr<buffer>& buf, const std::error_code& err){
				handler(conn, buf, err);
				w->resp();
			};

			std::size_t failed = 0;

			for(auto & conn : workers_)
			{
				buf.pkt()->pktcode = conn->pktcode();
				if (!conn->send(this, buf, [conn, &fn](std::shared_ptr<buffer>& buf, const std::error_code& err) mutable {
					fn(conn, buf, err);
					}))
				{
					++failed;
				}
			}

			waitable_->wait_for(failed, std::chrono::seconds{1000}, label);

			return workers_.size();
		}

		std::size_t size() const noexcept
		{
			return workers_.size();
		}
	private:
		std::shared_ptr<waitable> waitable_;
		container workers_;
	};


	template<typename Container>
	basic_transfer<Container> make_transfer(Container&& workers, abs::nodelog_interface* nodelog)
	{
		return basic_transfer<Container>{std::forward<Container>(workers), nodelog};
	}

}

#endif