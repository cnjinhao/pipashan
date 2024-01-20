
 
#ifndef PIPASHAN_MD5_INCLUDED
#define PIPASHAN_MD5_INCLUDED
 
#include <cstdint>
#include <string>

namespace pipashan
{
	class md5generator
	{
	public:
		using digest_type = const std::uint8_t(*)[16];

		md5generator()
		{
			this->reset();
		}

		void reset()
		{
			//finalized_ = false;
			counter_[0] = 0;
			counter_[1] = 0;

			state_[0] = 0x67452301;
			state_[1] = 0xefcdab89;
			state_[2] = 0x98badcfe;
			state_[3] = 0x10325476;
		}
	
	// MD5 block update operation. Continues an MD5 message-digest
		// operation, processing another message block
		void update(const std::uint8_t* data, std::size_t len)
		{
			// compute number of bytes mod 64
			auto index = counter_[0] / 8 % blocksize;
			
			// Update number of bits
			if ((counter_[0] += static_cast<std::uint32_t>(len << 3)) < (len << 3))
				counter_[1]++;
			counter_[1] += static_cast<std::uint32_t>(len >> 29);
			
			// number of bytes we need to fill in buffer
			auto firstpart = blocksize - index;
			
			std::size_t i;
			
			// transform as many times as possible.
			if (len >= firstpart)
			{
				// fill buffer first, transform
				std::memcpy(&blocks_[index], data, firstpart);
				this->_m_transform(blocks_);
			
				// transform chunks of blocksize (64 bytes)
				for (i = firstpart; i + blocksize <= len; i += blocksize)
					this->_m_transform(data + i);
			
				index = 0;
			}
			else
				i = 0;
			
			// buffer remaining input
			std::memcpy(&blocks_[index], data + i, len - i);
		}

		void finalize()
		{
			static std::uint8_t padding[blocksize] = {
				0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
			};

			//if (!finalized_)
			{
				// Save number of bits
				std::uint8_t bits[8];
				_m_encode(bits, counter_, 8);

				// pad out to 56 mod 64.
				auto index = counter_[0] / 8 % blocksize;
				auto padLen = (index < 56) ? (56 - index) : (120 - index);
				update(padding, padLen);

				// Append length (before padding)
				update(bits, 8);

				// Store state in digest
				_m_encode(digest_, state_, 16);

				// Zeroize sensitive information.
				//memset(buffer, 0, sizeof buffer);
				//memset(count, 0, sizeof count);

				//finalized_ = true;
			}
		}

		digest_type digest() const noexcept
		{
			return &digest_;
		}

		std::string hexdigest() const
		{
			std::string s;
			//if(finalized_)
			{
				static const char* table = "0123456789abcdef";
				s.reserve(32);
				for(int i = 0; i < 16; ++i)
				{
					s += table[digest_[i] >> 4];
					s += table[digest_[i] & 0xF];
				}
			}

			return s;
		}
	private:

		static void _m_encode(std::uint8_t* output, const std::uint32_t* input, std::size_t len)
		{
			for (std::size_t i = 0, j = 0; j < len; i++, j += 4) {
				output[j] = input[i] & 0xff;
				output[j+1] = (input[i] >> 8) & 0xff;
				output[j+2] = (input[i] >> 16) & 0xff;
				output[j+3] = (input[i] >> 24) & 0xff;
			}
		}

		static void _m_decode(std::uint32_t* output, const std::uint8_t* input)
		{
			for (std::size_t i = 0, j = 0; j < blocksize; i++, j += 4)
				output[i] = (input[j]) | 
							(static_cast<std::uint32_t>(input[j+1]) << 8) |
							(static_cast<std::uint32_t>(input[j+2]) << 16) |
							(static_cast<std::uint32_t>(input[j+3]) << 24);
		}

		//The length of blocks is at least blocksize
		void _m_transform(const std::uint8_t* blocks)
		{
			std::uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3], x[16];
			_m_decode(x, blocks);

			/* Round 1 */
			_m_FF (a, b, c, d, x[ 0], S11, 0xd76aa478); /* 1 */
			_m_FF (d, a, b, c, x[ 1], S12, 0xe8c7b756); /* 2 */
			_m_FF (c, d, a, b, x[ 2], S13, 0x242070db); /* 3 */
			_m_FF (b, c, d, a, x[ 3], S14, 0xc1bdceee); /* 4 */
			_m_FF (a, b, c, d, x[ 4], S11, 0xf57c0faf); /* 5 */
			_m_FF (d, a, b, c, x[ 5], S12, 0x4787c62a); /* 6 */
			_m_FF (c, d, a, b, x[ 6], S13, 0xa8304613); /* 7 */
			_m_FF (b, c, d, a, x[ 7], S14, 0xfd469501); /* 8 */
			_m_FF (a, b, c, d, x[ 8], S11, 0x698098d8); /* 9 */
			_m_FF (d, a, b, c, x[ 9], S12, 0x8b44f7af); /* 10 */
			_m_FF (c, d, a, b, x[10], S13, 0xffff5bb1); /* 11 */
			_m_FF (b, c, d, a, x[11], S14, 0x895cd7be); /* 12 */
			_m_FF (a, b, c, d, x[12], S11, 0x6b901122); /* 13 */
			_m_FF (d, a, b, c, x[13], S12, 0xfd987193); /* 14 */
			_m_FF (c, d, a, b, x[14], S13, 0xa679438e); /* 15 */
			_m_FF (b, c, d, a, x[15], S14, 0x49b40821); /* 16 */

			/* Round 2 */
			_m_GG (a, b, c, d, x[ 1], S21, 0xf61e2562); /* 17 */
			_m_GG (d, a, b, c, x[ 6], S22, 0xc040b340); /* 18 */
			_m_GG (c, d, a, b, x[11], S23, 0x265e5a51); /* 19 */
			_m_GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa); /* 20 */
			_m_GG (a, b, c, d, x[ 5], S21, 0xd62f105d); /* 21 */
			_m_GG (d, a, b, c, x[10], S22,  0x2441453); /* 22 */
			_m_GG (c, d, a, b, x[15], S23, 0xd8a1e681); /* 23 */
			_m_GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8); /* 24 */
			_m_GG (a, b, c, d, x[ 9], S21, 0x21e1cde6); /* 25 */
			_m_GG (d, a, b, c, x[14], S22, 0xc33707d6); /* 26 */
			_m_GG (c, d, a, b, x[ 3], S23, 0xf4d50d87); /* 27 */
			_m_GG (b, c, d, a, x[ 8], S24, 0x455a14ed); /* 28 */
			_m_GG (a, b, c, d, x[13], S21, 0xa9e3e905); /* 29 */
			_m_GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8); /* 30 */
			_m_GG (c, d, a, b, x[ 7], S23, 0x676f02d9); /* 31 */
			_m_GG (b, c, d, a, x[12], S24, 0x8d2a4c8a); /* 32 */

			/* Round 3 */
			_m_HH (a, b, c, d, x[ 5], S31, 0xfffa3942); /* 33 */
			_m_HH (d, a, b, c, x[ 8], S32, 0x8771f681); /* 34 */
			_m_HH (c, d, a, b, x[11], S33, 0x6d9d6122); /* 35 */
			_m_HH (b, c, d, a, x[14], S34, 0xfde5380c); /* 36 */
			_m_HH (a, b, c, d, x[ 1], S31, 0xa4beea44); /* 37 */
			_m_HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9); /* 38 */
			_m_HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60); /* 39 */
			_m_HH (b, c, d, a, x[10], S34, 0xbebfbc70); /* 40 */
			_m_HH (a, b, c, d, x[13], S31, 0x289b7ec6); /* 41 */
			_m_HH (d, a, b, c, x[ 0], S32, 0xeaa127fa); /* 42 */
			_m_HH (c, d, a, b, x[ 3], S33, 0xd4ef3085); /* 43 */
			_m_HH (b, c, d, a, x[ 6], S34,  0x4881d05); /* 44 */
			_m_HH (a, b, c, d, x[ 9], S31, 0xd9d4d039); /* 45 */
			_m_HH (d, a, b, c, x[12], S32, 0xe6db99e5); /* 46 */
			_m_HH (c, d, a, b, x[15], S33, 0x1fa27cf8); /* 47 */
			_m_HH (b, c, d, a, x[ 2], S34, 0xc4ac5665); /* 48 */

			/* Round 4 */
			_m_II (a, b, c, d, x[ 0], S41, 0xf4292244); /* 49 */
			_m_II (d, a, b, c, x[ 7], S42, 0x432aff97); /* 50 */
			_m_II (c, d, a, b, x[14], S43, 0xab9423a7); /* 51 */
			_m_II (b, c, d, a, x[ 5], S44, 0xfc93a039); /* 52 */
			_m_II (a, b, c, d, x[12], S41, 0x655b59c3); /* 53 */
			_m_II (d, a, b, c, x[ 3], S42, 0x8f0ccc92); /* 54 */
			_m_II (c, d, a, b, x[10], S43, 0xffeff47d); /* 55 */
			_m_II (b, c, d, a, x[ 1], S44, 0x85845dd1); /* 56 */
			_m_II (a, b, c, d, x[ 8], S41, 0x6fa87e4f); /* 57 */
			_m_II (d, a, b, c, x[15], S42, 0xfe2ce6e0); /* 58 */
			_m_II (c, d, a, b, x[ 6], S43, 0xa3014314); /* 59 */
			_m_II (b, c, d, a, x[13], S44, 0x4e0811a1); /* 60 */
			_m_II (a, b, c, d, x[ 4], S41, 0xf7537e82); /* 61 */
			_m_II (d, a, b, c, x[11], S42, 0xbd3af235); /* 62 */
			_m_II (c, d, a, b, x[ 2], S43, 0x2ad7d2bb); /* 63 */
			_m_II (b, c, d, a, x[ 9], S44, 0xeb86d391); /* 64 */

			state_[0] += a;
			state_[1] += b;
			state_[2] += c;
			state_[3] += d;

			// Zeroize sensitive information.
			std::memset(x, 0, sizeof x);
		}
	private:
		// F, G, H and I are basic MD5 functions.
		static std::uint32_t _m_F(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
			return x&y | ~x&z;
		}
		
		static std::uint32_t _m_G(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
			return x&z | y&~z;
		}
		
		static std::uint32_t _m_H(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
			return x^y^z;
		}
		
		static std::uint32_t _m_I(std::uint32_t x, std::uint32_t y, std::uint32_t z) {
			return y ^ (x | ~z);
		}
		
		// rotate_left rotates x left n bits.
		static std::uint32_t _m_rotate_left(std::uint32_t x, int n) {
			return (x << n) | (x >> (32-n));
		}
		
		// FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4.
		// Rotation is separate from addition to prevent recomputation.
		static void _m_FF(std::uint32_t &a, std::uint32_t b, std::uint32_t c, std::uint32_t d, std::uint32_t x, std::uint32_t s, std::uint32_t ac) {
			a = _m_rotate_left(a+ _m_F(b,c,d) + x + ac, s) + b;
		}
		
		static void _m_GG(std::uint32_t &a, std::uint32_t b, std::uint32_t c, std::uint32_t d, std::uint32_t x, std::uint32_t s, std::uint32_t ac) {
			a = _m_rotate_left(a + _m_G(b,c,d) + x + ac, s) + b;
		}
		
		static void _m_HH(std::uint32_t &a, std::uint32_t b, std::uint32_t c, std::uint32_t d, std::uint32_t x, std::uint32_t s, std::uint32_t ac) {
			a = _m_rotate_left(a + _m_H(b,c,d) + x + ac, s) + b;
		}
		
		static void _m_II(std::uint32_t &a, std::uint32_t b, std::uint32_t c, std::uint32_t d, std::uint32_t x, std::uint32_t s, std::uint32_t ac) {
			a = _m_rotate_left(a + _m_I(b,c,d) + x + ac, s) + b;
		}
	private:
		static constexpr std::size_t blocksize = 64;
		// Constants for MD5Transform routine.
		static constexpr std::uint32_t S11 = 7;
		static constexpr std::uint32_t S12 = 12;
		static constexpr std::uint32_t S13 = 17;
		static constexpr std::uint32_t S14 = 22;
		static constexpr std::uint32_t S21 = 5;
		static constexpr std::uint32_t S22 = 9;
		static constexpr std::uint32_t S23 = 14;
		static constexpr std::uint32_t S24 = 20;
		static constexpr std::uint32_t S31 = 4;
		static constexpr std::uint32_t S32 = 11;
		static constexpr std::uint32_t S33 = 16;
		static constexpr std::uint32_t S34 = 23;
		static constexpr std::uint32_t S41 = 6;
		static constexpr std::uint32_t S42 = 10;
		static constexpr std::uint32_t S43 = 15;
		static constexpr std::uint32_t S44 = 21;

		//bool finalized_;
		std::uint8_t blocks_[blocksize]; // bytes that didn't fit in last 64 byte chunk
		std::uint32_t counter_[2];	// 64bit counter for number of bits (lo, hi)
		std::uint32_t state_[4]; // digest so far
		std::uint8_t digest_[16]; // the result
	};

	std::string md5_old(const std::string& str);

	inline std::string md5(const std::string& str)
	{
		md5generator md5;

		md5.update(reinterpret_cast<const std::uint8_t*>(str.data()), str.size());
		md5.finalize();
	
		return md5.hexdigest();
	}
}
 
#endif