#ifndef PIPASHAN_SERIALIZER_INCLUDED
#define PIPASHAN_SERIALIZER_INCLUDED

#include <string>
#include <stdexcept>

namespace pipashan
{
	class serialization
	{
	public:
		template<typename ...Args>
		void serialize(Args&& ...args)
		{
			std::size_t len = 0;
			((len += _m_bytes(std::forward<Args>(args))), ...);

			data_.reserve(data_.size() + len);

			((_m_serialize(std::forward<Args>(args))), ...);
		}

		const std::string& data() const
		{
			return data_;
		}

		std::string data()
		{
			return std::move(data_);
		}
	private:
		template<typename Arg>
		static std::size_t _m_bytes(Arg&& arg)
		{
			if constexpr(std::is_integral_v<std::remove_cvref_t<Arg>> || std::is_unsigned_v<std::remove_cvref_t<Arg>>)
			{
				return sizeof(arg);
			}
			else if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, std::string> || std::is_same_v<std::remove_cvref_t<Arg>, std::string_view>)
			{
				return _m_strlen(arg);
			}
			else if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, char*>)
			{
				return _m_strlen(std::string_view{arg, std::strlen(arg)});
			}
			else if constexpr(std::is_trivially_copyable_v<std::remove_cvref_t<Arg>>)
			{
				return sizeof(arg);
			}
			else
			{
				int * p = & arg;
				throw std::invalid_argument("Type not supported");
			}
		}

		template<typename Arg, typename ...Parameters>
		static std::size_t _m_bytes(std::vector<Arg, Parameters...>& vec)
		{
			auto len = _m_capacity_space(vec.size());
			for(auto & m : vec)
				len += _m_bytes(m);
			return len;
		}

		template<typename Arg>
		void _m_serialize(Arg&& arg)
		{
			if constexpr(std::is_integral_v<std::remove_cvref_t<Arg>> || std::is_unsigned_v<std::remove_cvref_t<Arg>> || std::is_trivially_copyable_v<std::remove_cvref_t<Arg>>)
			{
				data_.append(reinterpret_cast<const char*>(&arg), sizeof(arg));
			}
			else if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, std::string> || std::is_same_v<std::remove_cvref_t<Arg>, std::string_view>)
			{
				_m_serialize_string(std::string_view{arg.data(), arg.size()});
			}
			else if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, char*>)
			{
				_m_serialize_string(std::string_view{arg, std::strlen(arg)});
			}
			else
			{
				int * p = &arg;
				throw std::invalid_argument{"Type not supported"};
			}
		}

		template<typename Arg>
		void _m_serialize(std::vector<Arg>& vec)
		{
			_m_serialize_capacity(vec.size());

			for(auto & m : vec)
				_m_serialize(m);
		}

		template<typename String>
		static std::size_t _m_strlen(String&& str)
		{
			if(str.size() <= 0x7F)
				return 1 + str.size();
			else if(str.size() <= 0x3FFF)
				return 2 + str.size();
			else if(str.size() <= 0x1FFFFF)
				return 3 + str.size();
			else if(str.size() <= 0xFFFFFFF)
				return 4 + str.size();

			throw std::invalid_argument("string is too long too support");
		}

		static std::size_t _m_capacity_space(std::size_t size)
		{
			if(size <= 0x7F)
				return 1;
			else if(size <= 0x3FFF)
				return 2;
			else if(size <= 0x1FFFFF)
				return 3;
			else if(size <= 0xFFFFFFF)
				return 4;

			throw std::invalid_argument("string is too long too support");
		}

		void _m_serialize_capacity(std::size_t size)
		{
			if(size <= 0x7F)
			{
				auto len = static_cast<std::uint8_t>(size);
				data_.append(reinterpret_cast<const char*>(&len), sizeof(len));
			}
			else if(size <= 0x3FFF)
			{
				std::uint16_t len = (1 << 15) | static_cast<std::uint16_t>(size);

				std::uint8_t bytes[2];
				bytes[0] = len >> 8;
				bytes[1] = len & 0xFF;

				for(std::size_t i = 0; i < sizeof(bytes); ++i)
					data_.append(reinterpret_cast<const char*>(&bytes[i]), 1);
			}
			else if(size <= 0x1FFFFF)
			{
				std::uint32_t len = (3 << 22) | static_cast<std::uint32_t>(size);

				std::uint8_t bytes[3];
				bytes[0] = len >> 16;
				bytes[1] = len >> 8;
				bytes[2] = len & 0xFF;

				for(std::size_t i = 0; i < sizeof(bytes); ++i)
					data_.append(reinterpret_cast<const char*>(&bytes[i]), 1);
			}
			else if(size <= 0xFFFFFFF)
			{
				std::uint32_t len = (7 << 29) | static_cast<std::uint32_t>(size);
				std::uint8_t bytes[4];
				bytes[0] = len >> 24;
				bytes[1] = len >> 16;
				bytes[2] = len >> 8;
				bytes[3] = len & 0xFF;

				for(std::size_t i = 0; i < sizeof(bytes); ++i)
					data_.append(reinterpret_cast<const char*>(&bytes[i]), 1);
			}
			else
				throw std::invalid_argument("size is too long too support");
		}

		void _m_serialize_string(std::string_view str)
		{
			_m_serialize_capacity(str.size());
			data_.append(str);
		}
	private:
		std::string data_;
	};

	template<typename Arg>
	serialization& operator<<(serialization& s11n, Arg&& t)
	{
		s11n.serialize(std::forward<Arg>(t));
		return s11n;
	}

	class deserialization
	{
	public:
		deserialization(std::string_view sv):
			data_(sv)
		{
		}

		deserialization(std::string&& s):
			storage_(std::move(s))
		{
			data_ = std::string_view{storage_};
		}

		template<typename ...Args>
		bool deserialize(Args& ...args)
		{
			try
			{
				(_m_deserialize(args), ...);
			}
			catch(...)
			{
				return false;
			}
			
			return true;
		}
	private:
		std::size_t _m_capacity_size()
		{
			auto x = *reinterpret_cast<const std::uint8_t*>(data_.data() + pos_);

			if(0 == (x >> 7))
			{
				if(pos_ + 1 > data_.size())
					throw std::out_of_range("not enough data to deserialize");

				pos_ += 1;
				return (x & 0x7F);
			}
			else if(0x2 == (x >> 6))
			{
				if(pos_ + 2 > data_.size())
					throw std::out_of_range("not enough data to deserialize");

				std::size_t len = 0;
				len = static_cast<std::size_t>(x) << 8;
				len |= static_cast<std::uint8_t>(data_[pos_ + 1]);

				pos_ += 2;
				return len & 0x3FFF;
			}
			else if(0x6 == (x >> 5))
			{
				if(pos_ + 3 > data_.size())
					throw std::out_of_range("not enough data to deserialize");

				std::size_t len = (x & 0x1F) << 16;
				len |= static_cast<std::size_t>(static_cast<std::uint8_t>(data_[pos_ + 1])) << 8;
				len |= static_cast<std::uint8_t>(data_[pos_ + 2]);

				pos_ += 3;

				return len;
			}
			else if(0xE == (x >> 4))
			{
				if(pos_ + 4 > data_.size())
					throw std::out_of_range("not enough data to deserialize");

				std::size_t len = (x & 0xF) << 24;
				
				len |= static_cast<std::size_t>(static_cast<std::uint8_t>(data_[pos_ + 1])) << 16;
				len |= static_cast<std::size_t>(static_cast<std::uint8_t>(data_[pos_ + 2])) << 8;
				len |= static_cast<std::uint8_t>(data_[pos_ + 3]);

				pos_ += 4;
				return len;
			}

			return 0;
		}

		template<typename Arg>
		void _m_deserialize(Arg & arg)
		{
			if(pos_ >= data_.size())
				throw std::out_of_range("Not enough data to deserialize");

			if constexpr(std::is_integral_v<std::remove_cvref_t<Arg>> || std::is_unsigned_v<std::remove_cvref_t<Arg>> ||std::is_trivially_copyable_v<std::remove_cvref_t<Arg>>)
			{
				if(pos_ + sizeof(arg) > data_.size())
					throw std::out_of_range("Not enough data to deserialize");

				arg = *reinterpret_cast<const Arg*>(data_.data() + pos_);
				pos_ += sizeof(arg);
			}
			else if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, std::string>)
			{
				auto len = _m_capacity_size();

				if(pos_ + len > data_.size())
					throw std::out_of_range("Not enough data to deserialize");

				arg.assign(data_.data() + pos_, len);
				pos_ += len;
			}
			else if constexpr(std::is_same_v<std::remove_cvref_t<Arg>, std::string_view>)
			{
				auto len = _m_capacity_size();

				if(pos_ + len > data_.size())
					throw std::out_of_range("Not enough data to deserialize");

				arg = std::string_view{data_.data() + pos_, len};
				pos_ += len;
			}
			else
			{
				throw std::invalid_argument("Type not supported");
			}			
		}

		template<typename Arg>
		void _m_deserialize(std::vector<Arg>& vec)
		{
			Arg arg;
			auto size = _m_capacity_size();
			for(std::size_t i = 0; i < size; ++i)
			{
				_m_deserialize(arg);
				vec.emplace_back(std::move(arg));
			}
		}

	private:
		std::string storage_;
		std::string_view data_;
		std::size_t pos_{ 0 };
	};

	template<typename Arg>
	deserialization& operator>>(deserialization& d13n, Arg& t)
	{
		d13n.deserialize(t);
		return d13n;
	}
}

#endif