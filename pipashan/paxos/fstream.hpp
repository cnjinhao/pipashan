#ifndef PIPASHAN_PAXOS_FSTREAM_INCLUDED
#define PIPASHAN_PAXOS_FSTREAM_INCLUDED

#include "../c++defines.hpp"

#include <filesystem>

#ifdef PIPASHAN_OS_WINDOWS
#include <windows.h>
#else

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#endif

namespace pipashan::paxos_details
{
	class fstream
	{
		using dummy_bool = void(*)(fstream*);

#ifndef PIPASHAN_OS_WINDOWS
		static constexpr mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
#endif
	public:
		using file_size_t = std::uint64_t;

		fstream() = default;

		fstream(std::filesystem::path p):
#ifdef PIPASHAN_OS_WINDOWS
			fd_(::CreateFileW(p.wstring().data(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr)),
			path_(std::move(p))
		{
			if(INVALID_HANDLE_VALUE != fd_)
			{
				LARGE_INTEGER li;
				if(!::GetFileSizeEx(fd_, &li))
				{
					::CloseHandle(fd_);
					fd_ = INVALID_HANDLE_VALUE;
					return;
				}
				bytes_ = static_cast<std::size_t>(li.QuadPart);
			}
		}
#else
			fd_(::open(p.string().c_str(), O_RDWR | O_NONBLOCK | O_CREAT, mode)),
			path_(std::move(p))
		{
			if(-1 == fd_)
			{
				error_code_ = errno;
				return;
			}
			
			bytes_ = ::lseek(fd_, 0, SEEK_END);
			::lseek(fd_, 0, SEEK_SET);
		}
#endif

		~fstream()
		{
			close();
		}

		std::string error_message() const
		{
#ifdef PIPASHAN_OS_WINDOWS
#else
			switch(error_code_)
			{
			case 0:			return {};
			case EACCES:	return "Permission denied";
			}
#endif
			return "error code = " + std::to_string(error_code_);
		}

		void close()
		{
#ifdef PIPASHAN_OS_WINDOWS
			if(INVALID_HANDLE_VALUE != fd_)
				::CloseHandle(fd_);
			
			fd_ = INVALID_HANDLE_VALUE;
#else
			if(-1 != fd_)
				::close(fd_);
			fd_ = -1;
#endif
			path_.clear();
		}

		bool is_open() const noexcept
		{
#ifdef PIPASHAN_OS_WINDOWS
			return (INVALID_HANDLE_VALUE != fd_);
#else
			return (-1 != fd_);
#endif
		}

		const std::filesystem::path& filename() const
		{
			return path_;
		}

		/// 打开
		bool open(std::filesystem::path p)
		{
			close();
#ifdef PIPASHAN_OS_WINDOWS
			fd_ = ::CreateFileW(p.wstring().data(),
								GENERIC_READ | GENERIC_WRITE,
								FILE_SHARE_READ,
								nullptr,
								OPEN_ALWAYS,
								FILE_ATTRIBUTE_NORMAL, nullptr);

			if(INVALID_HANDLE_VALUE != fd_)
			{
				LARGE_INTEGER li;
				if(!::GetFileSizeEx(fd_, &li))
				{
					::CloseHandle(fd_);
					fd_ = INVALID_HANDLE_VALUE;
					return;
				}
				bytes_ = static_cast<std::size_t>(li.QuadPart);
				position_ = 0;
				path_.swap(p);
				return true;
			}				

#else			
			fd_ = ::open(p.string().c_str(), O_RDWR | O_NONBLOCK | O_CREAT, mode);
			if(-1 != fd_)
			{
				bytes_ = ::lseek(fd_, 0, SEEK_END);
				::lseek(fd_, 0, SEEK_SET);
				position_ = 0;
				path_.swap(p);
				return true;
			}

			error_code_ = errno;
#endif
			return false;
		}

		void clear()
		{
			//Keep the path, close() always clear the path_
			auto p = std::move(path_);
			this->close();

			std::error_code err;
			std::filesystem::remove(p, err);

			open(p);
		}

		void seek(std::int64_t pos)
		{
#ifdef PIPASHAN_OS_WINDOWS
			LARGE_INTEGER distance, li;
			distance.QuadPart = pos;
			if(::SetFilePointerEx(fd_, distance, &li, FILE_BEGIN))
				position_ = static_cast<std::size_t>(li.QuadPart);
#else
			position_ = ::lseek(fd_, static_cast<off_t>(pos), SEEK_SET);
#endif
		}

		void seek(off_t offset, int where)
		{
#ifdef PIPASHAN_OS_WINDOWS
			LARGE_INTEGER distance, li;
			distance.QuadPart = offset;

			if(where == SEEK_SET)
				where = FILE_BEGIN;
			else if(where == SEEK_CUR)
				where = FILE_CURRENT;
			else if(where == SEEK_END)
				where = FILE_END;

			if(::SetFilePointerEx(fd_, distance, &li, where))
				position_ = static_cast<std::size_t>(li.QuadPart);
#else
			position_ = ::lseek(fd_, offset, where);
#endif
		}

		/// Seeks to the end of file
		file_size_t seekend()
		{
#ifdef PIPASHAN_OS_WINDOWS
			LARGE_INTEGER distance, li;
			distance.QuadPart = 0;
			if(::SetFilePointerEx(fd_, distance, &li, FILE_END))
				position_ = static_cast<std::size_t>(li.QuadPart);
#else
			position_ = ::lseek(fd_, 0, SEEK_END);
#endif
			return position_;
		}

		file_size_t tellp() const
		{
#ifdef PIPASHAN_OS_WINDOWS
			LARGE_INTEGER distance, li;
			distance.QuadPart = 0;
			if(::SetFilePointerEx(fd_, distance, &li, FILE_CURRENT))
				return static_cast<file_size_t>(li.QuadPart);

			return 0;
#else
			return ::lseek(fd_, 0, SEEK_CUR);
#endif
		}

		file_size_t read(void* data, file_size_t len)
		{
#ifdef PIPASHAN_OS_WINDOWS
			DWORD readbytes;
			if(::ReadFile(fd_, data, static_cast<DWORD>(len), &readbytes, nullptr))
				position_ = tellp();

			return readbytes;
#else
			auto s = ::read(fd_, data, len);
			position_ = ::lseek(fd_, 0, SEEK_CUR);
			return s;
#endif
		}

		file_size_t write(const void* data, file_size_t len)
		{
#ifdef PIPASHAN_OS_WINDOWS
			DWORD written_size;
			if(!::WriteFile(fd_, data, static_cast<DWORD>(len), &written_size, nullptr))
				return 0;
#else
			auto written_size = ::write(fd_, data, len);
#endif
			if(written_size + position_ > bytes_)
				bytes_ = position_ + written_size;

			position_ += written_size;
			
			return written_size;
		}

		void truncate(file_size_t pos)
		{
#ifdef PIPASHAN_OS_WINDOWS
			LARGE_INTEGER li;
			li.QuadPart = static_cast<long long>(pos);
			::SetFilePointerEx(fd_, li, nullptr, FILE_BEGIN);
			::SetEndOfFile(fd_);
#else
			::ftruncate(fd_, pos);
#endif
			bytes_ = pos;
			if(position_ > pos)
				seek(pos);
		}

		void fsync()
		{
#ifdef PIPASHAN_OS_WINDOWS
			::FlushFileBuffers(fd_);
#else
			::fsync(fd_);
#endif
		}

		/// 返回文件总字节数
		file_size_t bytes() const
		{
			return bytes_;
		}

		operator dummy_bool() const
		{
#ifdef PIPASHAN_OS_WINDOWS
			return INVALID_HANDLE_VALUE != fd_ ? reinterpret_cast<dummy_bool>(1): nullptr;
#else
			return fd_ > -1 ? reinterpret_cast<dummy_bool>(1): nullptr;
#endif
		}
	private:
#ifdef PIPASHAN_OS_WINDOWS
		HANDLE fd_{ INVALID_HANDLE_VALUE };
#else
		int fd_{ -1 };
#endif
		std::filesystem::path path_;
		file_size_t bytes_{ 0 };
		file_size_t position_{ 0 };
		int error_code_{ 0 };
	};
}

#endif