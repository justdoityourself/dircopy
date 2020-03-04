/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>

#include "d8u/transform.hpp"
#include "tdb/database.hpp"

namespace dircopy
{
	namespace delta
	{
		using namespace d8u::transform;

		class Path
		{
			tdb::TinyHashmapSafe change;
			tdb::TinyHashmapSafe previous;
			tdb::TinyHashmapSafe current;

			std::string root;
		public:
			Path(std::string_view _root)
				: change(string(_root) + "/change.db")
				, previous(string(_root) + "/latest.db")
				, current(string(_root) + "/tmp.db")
				, root ( _root )
			{ }

			std::string Finalize()
			{
				change.Close();
				previous.Close();
				current.Close();

				std::error_code err;
				if (!std::filesystem::remove(string(root) + "/latest.db"),err)
					throw std::runtime_error(err.message());
				std::filesystem::rename(string(root) + "/tmp.db", string(root) + "/latest.db", err);
				if(err)
					throw std::runtime_error(err.message());

				return string(root) + "/latest.db";
			}

			uint8_t* Queue(std::string_view s, uint64_t size, uint64_t when, uint64_t BLOCK, uint64_t MAX)
			{
				auto key_payload = (size > MAX) ? 32 : 32 * (size / BLOCK + 1 /*FILE HASH*/ + ((size % BLOCK) ? 1 : 0));

				auto b_size = bundle_size(s, size, when, key_payload);
				auto [queue, off] = current.Incidental(b_size);

				*(uint32_t*)(queue) = (uint32_t)b_size;

				current.Insert(s, off);

				return queue;
			}

			bool Changed(std::string_view s, uint64_t size, uint64_t when, uint8_t * queue )
			{
				auto [pointer,would_write] = change.Insert(s, when);

				if (would_write && *pointer == when)
				{
					auto data = previous.Find(s);
						
					if (!data)
						throw std::runtime_error("Bad delta state");

					auto ptr = previous.GetObject(*data);

					std::copy(ptr, ptr + *((uint32_t*)ptr), queue);

					return false;
				}

				return true;
			}

			template <typename T> void Apply(std::string_view s, uint64_t size, uint64_t when, const T& k, uint8_t * queue)
			{
				auto b_size = *(uint32_t*)queue;

				stream(queue, b_size, s, size, when, k);
			}

			static auto Decode(uint8_t* p)
			{
				uint32_t extent = *(uint32_t*)p;
				uint64_t size = *(uint64_t*)(p + 4);
				uint64_t time = *(uint64_t*)(p + 12);
				uint16_t ns = *(uint16_t*)(p + 20);

				std::string_view name((char*)(p + 22), ns);

				uint16_t ds = *(uint16_t*)(p + 22 + ns);

				span<DefaultHash> data((DefaultHash*)(p + 24+ns), ds / sizeof(DefaultHash));

				return std::make_tuple(size, time, name, data);
			}

		private:

			size_t bundle_size(std::string_view s, uint64_t size, uint64_t when, size_t k_size)
			{
				return sizeof(uint32_t) + sizeof(uint64_t) * 2 + sizeof(uint16_t) * 2 + s.size()  + k_size;
			}

			template <typename T> void stream(uint8_t* dest, size_t b_size, std::string_view s, uint64_t size, uint64_t when, const T& k)
			{
				*(uint32_t*)(dest) = (uint32_t)b_size;
				*(uint64_t*)(dest + 4) = (uint32_t)size;
				*(uint64_t*)(dest + 12) = (uint32_t)when;
				*(uint16_t*)(dest + 20) = (uint16_t)s.size();
				std::copy(s.begin(), s.end(), dest + 22);
				*(uint16_t*)(dest + 22 + s.size()) = (uint32_t)k.size();
				std::copy(k.begin(), k.end(), dest + 24 + s.size());
			}
		};
	}
}