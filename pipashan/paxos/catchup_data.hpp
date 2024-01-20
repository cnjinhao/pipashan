#ifndef PIPASHAN_PAXOS_CATCHUP_DATA_INCLUDED
#define PIPASHAN_PAXOS_CATCHUP_DATA_INCLUDED

#include <cstdint>

namespace pipashan::paxos_details
{
	class catchup_data
	{
		catchup_data(const catchup_data&) = delete;
		catchup_data& operator=(const catchup_data&) = delete;
	public:
		using file_size_t = proto::file_size_t;

		/// A catchup description which is at beginning of catchup data buffer.
		struct data_metrics
		{
			file_size_t internal_bytes{ 0 };	///< Bytes of internal snapshot
			file_size_t external_bytes{ 0 };	///< Bytes of external snapshot
			file_size_t anchors_bytes{0};
			file_size_t journal_bytes{ 0 };	///< Bytes of journal queue

			bool empty() const noexcept
			{
				return internal_bytes + external_bytes + anchors_bytes + journal_bytes == 0;
			}
		};

		catchup_data(std::size_t tasks):
			task_size_(tasks)
		{
		}

		catchup_data(catchup_data&& rhs):
			buf_(rhs.buf_),
			len_(rhs.len_),
			task_size_(rhs.task_size_)
		{
			rhs.buf_ = nullptr;
			rhs.len_ = 0;
			rhs.task_size_ = 0;
		}

		~catchup_data()
		{
			delete [] buf_;
		}

		catchup_data& operator=(catchup_data&& rhs)
		{
			if(this != &rhs)
			{
				delete [] buf_;
				buf_ = rhs.buf_;
				len_ = rhs.len_;
				task_size_ = rhs.task_size_;

				rhs.buf_ = nullptr;
				rhs.len_ = 0;
				rhs.task_size_ = 0;
			}
			return *this;
		}


		bool empty() const noexcept
		{
			return !buf_;
		}

		std::size_t bytes() const noexcept
		{
			return static_cast<std::size_t>(len_);
		}

		const std::uint8_t* data() const noexcept
		{
			return buf_;
		}

		std::size_t task_size() const noexcept
		{
			return task_size_;
		}

		void task_size(std::size_t n)
		{
			task_size_ = n;
		}

		void alloc(file_size_t internal_bytes, file_size_t external_bytes, file_size_t anchors_bytes, file_size_t journal_bytes)
		{
			delete [] buf_;
			buf_ = nullptr;

			len_ = sizeof(data_metrics) + internal_bytes + external_bytes + anchors_bytes + journal_bytes;

			buf_ = new std::uint8_t[static_cast<std::size_t>(len_)];

			//Initialize data_metrics

			auto dm = reinterpret_cast<data_metrics*>(buf_);

			dm->internal_bytes = internal_bytes;
			dm->external_bytes = external_bytes;
			dm->anchors_bytes = anchors_bytes;
			dm->journal_bytes = journal_bytes;
		}

		const data_metrics* metrics() const noexcept
		{
			return reinterpret_cast<const data_metrics*>(buf_);
		}

		std::uint8_t* internal()
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<data_metrics*>(buf_);
				if(dm->internal_bytes)
					return buf_ + sizeof(data_metrics);
			}
			return nullptr;
		}

		file_size_t internal_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<data_metrics*>(buf_)->internal_bytes;

			return 0;
		}

		std::uint8_t* external() noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<data_metrics*>(buf_);
				if(dm->external_bytes)
					return buf_ + sizeof(data_metrics) + dm->internal_bytes;
			}
			return nullptr;
		}

		file_size_t external_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<data_metrics*>(buf_)->external_bytes;

			return 0;
		}

		std::uint8_t* anchors() noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<data_metrics*>(buf_);
				if(dm->anchors_bytes)
					return buf_ + sizeof(data_metrics) + dm->internal_bytes + dm->external_bytes;
			}
			return nullptr;
		}

		file_size_t anchors_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<data_metrics*>(buf_)->anchors_bytes;

			return 0;
		}

		std::uint8_t* journal() noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<data_metrics*>(buf_);
				if(dm->journal_bytes)
					return buf_ + sizeof(data_metrics) + dm->internal_bytes + dm->external_bytes + dm->anchors_bytes;
			}
			return nullptr;
		}

		file_size_t journal_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<data_metrics*>(buf_)->journal_bytes;

			return 0;
		}
	private:
		std::uint8_t * buf_{ nullptr };
		file_size_t len_{ 0 };
		std::size_t task_size_;
	};

	class catchup_data_view
	{
	public:
		/// A catchup description which is at beginning of catchup data buffer.
		using data_metrics = catchup_data::data_metrics;
		using file_size_t = proto::file_size_t;

		catchup_data_view(const catchup_data& cdata):
			catchup_data_view(reinterpret_cast<const char*>(cdata.data()), cdata.bytes())
		{}


		catchup_data_view(const char* data, std::size_t len):
			buf_(data),
			len_(len)
		{
			if(len < sizeof(data_metrics))
			{
				buf_ = nullptr;
				len_ = 0;
				return;
			}

			auto dm = reinterpret_cast<const data_metrics*>(data);
			if(sizeof(data_metrics) + dm->internal_bytes + dm->external_bytes + dm->journal_bytes + dm->anchors_bytes > len)
			{
				buf_ = nullptr;
				len_ = 0;
				return;
			}
		}

		bool empty() const noexcept
		{
			return !buf_;
		}

		std::size_t bytes() const noexcept
		{
			return len_;
		}

		const data_metrics* metrics() const noexcept
		{
			return reinterpret_cast<const data_metrics*>(buf_);
		}

		const char* internal() const noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<const data_metrics*>(buf_);
				if(dm->internal_bytes)
					return buf_ + sizeof(data_metrics);
			}
			return nullptr;
		}

		file_size_t internal_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<const data_metrics*>(buf_)->internal_bytes;

			return 0;
		}

		const char* external() const noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<const data_metrics*>(buf_);
				if(dm->external_bytes)
					return buf_ + sizeof(data_metrics) + dm->internal_bytes;
			}
			return nullptr;
		}

		file_size_t external_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<const data_metrics*>(buf_)->external_bytes;

			return 0;
		}

		const char* anchors() const noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<const data_metrics*>(buf_);
				if(dm->anchors_bytes)
					return buf_ + sizeof(data_metrics) + dm->internal_bytes + dm->external_bytes;
			}
			return nullptr;
		}

		file_size_t anchors_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<const data_metrics*>(buf_)->anchors_bytes;

			return 0;
		}

		const char* journal() const noexcept
		{
			if(buf_)
			{
				auto dm = reinterpret_cast<const data_metrics*>(buf_);
				if(dm->journal_bytes)
					return buf_ + sizeof(data_metrics) + dm->internal_bytes + dm->external_bytes + dm->anchors_bytes;
			}
			return nullptr;
		}

		file_size_t journal_bytes() const noexcept
		{
			if(buf_)
				return reinterpret_cast<const data_metrics*>(buf_)->journal_bytes;

			return 0;
		}
	private:
		const char * buf_;
		std::size_t len_;
	};


}

#endif