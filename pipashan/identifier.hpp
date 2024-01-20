#ifndef PIPASHAN_IDENTIFIER_INCLUDED
#define PIPASHAN_IDENTIFIER_INCLUDED

#include "c++defines.hpp"
#include "modules/md5.hpp"
#include <stdio.h>
#include <filesystem>

namespace pipashan
{
	class identifier
	{
	public:
		identifier()
		{
			std::memset(data_, 0, 16);
		}

		identifier(const identifier& rhs)
		{
			std::memcpy(data_, rhs.data_, 16);
		}

		identifier(identifier&& rhs)
		{
			std::memcpy(data_, rhs.data_, 16);
		}

		identifier& operator=(const identifier& rhs)
		{
			if(this != &rhs)
				std::memcpy(data_, rhs.data_, 16);

			return *this;
		}

		identifier& operator=(identifier&& rhs)
		{
			if (this != &rhs)
				std::memcpy(data_, rhs.data_, 16);
			return *this;
		}

		identifier(std::string_view data)
		{
			md5generator md5;
			md5.update(reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
			md5.finalize();

			std::memcpy(data_, md5.digest(), 16);
		}

		template<typename ...Args>
		void hash(Args&& ...args)
		{
			md5generator md5;

			(_m_update(md5, std::forward<Args>(args)), ...);
			md5.finalize();

			std::memcpy(data_, md5.digest(), 16);
		}

		void assign(const char* data)
		{
			std::memcpy(data_, data, 16);
		}

		std::string hex() const
		{
			std::string s;

			static const char* table = "0123456789abcdef";
			s.reserve(32);
			for (int i = 0; i < 16; ++i)
			{
				s += table[data_[i] >> 4];
				s += table[data_[i] & 0xF];
			}

			return s;
		}

		std::string short_hex() const
		{
			std::string s;

			static const char* table = "0123456789abcdef";
			s.reserve(4);
			for (int i = 0; i < 3; ++i)
			{
				auto x = (data_[i] >> 4);
				auto y = (data_[i] & 0xF);

				s += table[data_[i] >> 4];
				s += table[data_[i] & 0xF];
			}

			return s;
		}

		static constexpr std::size_t size()
		{
			return 16;
		}

		const std::uint8_t* data() const noexcept
		{
			return data_;
		}

		std::uint8_t* data() noexcept
		{
			return data_;
		}

		bool operator==(const identifier& r) const noexcept
		{
			return reinterpret_cast<const std::uint64_t*>(data_)[0] == reinterpret_cast<const std::uint64_t*>(r.data_)[0] &&
				reinterpret_cast<const std::uint64_t*>(data_)[1] == reinterpret_cast<const std::uint64_t*>(r.data_)[1];
		}

		bool operator!=(const identifier& r) const noexcept
		{
			return !this->operator==(r);
		}

		bool operator<(const identifier& r) const noexcept
		{
			for (std::size_t i = 0; i < 16; ++i)
				if (data_[i] < r.data_[i])
					return true;
				else if (data_[i] > r.data_[i])
					return false;

			return false;
		}
	private:
		template<typename Arg>
		void _m_update(md5generator& md5, Arg&& arg)
		{
			auto constexpr is_string = std::is_same_v<std::string, std::remove_cvref_t<Arg>> || std::is_same_v<std::string_view, std::remove_cvref_t<Arg>>;

			if constexpr (is_string)
				md5.update(reinterpret_cast<const std::uint8_t*>(arg.data()), arg.size());
			else if constexpr(std::is_trivially_copyable_v<std::remove_cvref_t<Arg>>)
				md5.update(reinterpret_cast<const std::uint8_t*>(&arg), sizeof(arg));

			static_assert(is_string || std::is_trivially_copyable_v<std::remove_cvref_t<Arg>>, "Unspported type");
		}

	private:
		std::uint8_t data_[16];
	};


	inline identifier make_id(std::string_view data)
	{
		return identifier{data};
	}
}

#endif