#ifndef PIPASHAN_EXECUTE_INCLUDED
#define PIPASHAN_EXECUTE_INCLUDED

#include <list>
#include <atomic>
#include <thread>
#include <mutex>

namespace pipashan
{
	template<typename T>
	class timer
	{
	public:
		timer(std::atomic<T>& val) :
			val_(val),
			tm_(std::chrono::high_resolution_clock::now())
		{
		}

		~timer()
		{
			val_ += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tm_).count();
		}

		std::uint64_t elapse() const
		{
			return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tm_).count();
		}
	private:
		std::atomic<T>& val_;
		std::chrono::high_resolution_clock::time_point tm_;
	};



	class execute
	{
		class task_if
		{
		public:
			virtual ~task_if() = default;
			virtual void run() = 0;
		};

		template<typename Function>
		class task: public task_if
		{
		public:
			task(Function&& func) :
				func_(std::move(func))
			{}

			void run() override
			{
				if constexpr (std::is_void<std::invoke_result_t<Function>>::value)
				{
					func_();
				}
				else
				{
					while (true)
					{
						auto s = func_();
						if (s.count() == 0)
							break;
						std::this_thread::sleep_for(s);
					}
				}
			}
		private:
			Function func_;
		};

		struct worker
		{
			std::mutex mutex;
			std::condition_variable condvar;

			std::unique_ptr<std::thread> thread;
			std::unique_ptr<task_if> task;

			~worker()
			{
				{
					std::lock_guard lock{ mutex };
					condvar.notify_one();
				}
				thread->join();
			}
		};
	public:
		execute()
		{
			for (std::size_t i = 0; i < 6; ++i)
			{
				idle_workers_.push_back(_m_make_worker());
			}
		}

		~execute()
		{
			working_ = false;

			while (true)
			{
				std::lock_guard lock{ mutex_ };
				for(auto wp : idle_workers_)
					delete wp;

				idle_workers_.clear();

				if (working_size_ == 0)
					break;
			}

		}

		template<typename Function>
		void run(Function&& fn)
		{
			worker * wp = nullptr;

			{
				std::lock_guard lock{ mutex_ };

				if (!working_)
					return;

				++working_size_;

				if (idle_workers_.empty())
				{
					wp = _m_make_worker();
				}
				else
				{
					wp = idle_workers_.front();
					idle_workers_.pop_front();
				}
			}

			std::lock_guard lock{ wp->mutex };
			wp->task.reset(new task<Function>(std::forward<Function>(fn)));
			wp->condvar.notify_one();
		}
	private:
		worker* _m_make_worker()
		{
			auto wp = new worker;
			wp->thread.reset(new std::thread{ [this, wp] {
				_m_worker(wp);
			}});
			return wp;
		}

		void _m_worker(worker* wp)
		{
			while (working_)
			{
				{
					std::unique_lock lock{ wp->mutex };
					if (!wp->task)
						wp->condvar.wait(lock);

					if ((!working_) || !wp->task)
						break;
				}

				try
				{
					wp->task->run();
				}
				catch (...)
				{
				}

				wp->task.reset();

				std::lock_guard lock{ mutex_ };
				idle_workers_.push_back(wp);
				--working_size_;
			}
		}
	private:
		std::recursive_mutex mutex_;
		std::atomic<bool> working_{ true };
		std::atomic<std::size_t> working_size_{ 0 };
		std::list<worker*> idle_workers_;
	};
}
#endif