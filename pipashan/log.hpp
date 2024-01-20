#ifndef PIPASHAN_LOG_INCLUDED
#define PIPASHAN_LOG_INCLUDED

#include <string>
#include <sstream>
#include <fstream>
#include <thread>
#include "proto.hpp"

namespace pipashan
{
	namespace log_details
	{
		class logio
		{
			logio& operator=(const logio&) = delete;
			logio& operator=(logio&&) = delete;

			logio(const logio&) = delete;
			logio(logio&&) = delete;
		public:
			logio(std::filesystem::path p):
				os_(p / "pipashan.log", std::ios::app)
			{
				thread_.reset(new std::thread{[this]{

					while(true)
					{
						{
							std::unique_lock lock{ cndvarmutex_ };
							if(!running_)
								break;

							if(!run_sync_)
							{
								cndvar_.wait(lock);
								if(!running_)
									break;
							}

							if(!run_sync_)
								continue;

							run_sync_ = false;
						}

						std::lock_guard lock{ mutex_ };
						os_.flush();
					}

					if(run_sync_)
					{
						std::lock_guard lock{ mutex_ };
						os_.flush();
					}

				}});
			}

			~logio()
			{
				{
					std::lock_guard lock{ cndvarmutex_ };
					running_ = false;

					cndvar_.notify_one();
				}
				
				thread_->join();
			}

			std::recursive_mutex& mutex()
			{
				return mutex_;
			}
		
			template<typename Arg>
			void write(Arg&& arg)
			{
				if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, proto::payload::address>)
				{
					if(!arg.ipv4.empty())
					{
						os_<<"inet "<<arg.ipv4;

						if(!arg.ipv6.empty())
							os_<<"/inet6 "<<arg.ipv6;
					}
					else if(!arg.ipv6.empty())
						os_<<"inet6 "<<arg.ipv6;

					os_<<" :"<<arg.port;
				}
				else
					os_<<std::forward<Arg>(arg);
			}

			void sync()
			{
				os_<<"\n";

				std::lock_guard lock{ cndvarmutex_ };
				run_sync_ = true;
				cndvar_.notify_one();
			}
		private:
			std::recursive_mutex mutex_;
			std::ofstream os_;

			std::mutex cndvarmutex_;
			std::condition_variable cndvar_;

			bool run_sync_{ false };
			bool running_{ true };

			std::unique_ptr<std::thread> thread_;
		};
	}



	class log
	{
	public:
		log(const std::string& title, log_details::logio& logio):
			title_("[" + title + "] "),
			logio_(logio)
		{
		}

		log(const std::string& paxos, const std::string& id, const std::string& title, log_details::logio& logio):
			title_("[" + paxos + ":" + id + "@" + title + "] "),
			logio_(logio)
		{
		}

		template<typename ...Args>
		void msg(Args&& ...args)
		{
			std::lock_guard lock{ logio_.mutex() };

			logio_.write(title_);
			(logio_.write(std::forward<Args>(args)), ...);
			logio_.sync();
		}

		template<typename ...Args>
		void err(Args&& ...args)
		{
			std::lock_guard lock{ logio_.mutex() };

			logio_.write(title_ + "<err> ");
			(logio_.write(std::forward<Args>(args)), ...);
			logio_.sync();
		}


	private:
		std::string const title_;
		log_details::logio & logio_;
	};

	namespace abs
	{
		class nodelog_interface
		{
		public:
			virtual ~nodelog_interface() = default;
			virtual pipashan::log log(const std::string& title) = 0;
		};
	}
}

#endif