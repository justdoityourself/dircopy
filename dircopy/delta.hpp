/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>

#include "d8u/transform.hpp"
#include "d8u/util.hpp"
#include "d8u/json.hpp"
#include "tdb/legacy.hpp"

namespace dircopy
{
	namespace delta
	{
		using namespace d8u::transform;
		using namespace d8u::json;

		template < typename TH = _DefaultHash> class Path
		{
			tdb::TinyHashmapSafe change;
			tdb::TinyHashmapSafe previous;
			tdb::TinyHashmapSafe current;

			JsonMap exclude;

			std::string root;
		public:
			Path(std::string_view _root,std::string_view _exclude)
				: change(string(_root) + "/change.db")
				, previous(string(_root) + "/latest.db")
				, current(string(_root) + "/tmp.db")
				, root ( _root )
				, exclude(_exclude)
			{ 
				if (std::filesystem::exists(string(_root) + "/lock.db"))
					throw std::runtime_error("Change Tracking database is locked, is a backup running? Did a backup fail to complete gracefully? If the second is true please delete the folder and try again.");
			}

			~Path() { }

			std::string Root() { return root; }

			struct FolderStatistics
			{
				uint64_t target;
				uint64_t size;
				uint64_t blocks;
				uint64_t files;
			};

			template<typename T> void Statistics(d8u::util::Statistics& _stats, const T& domain)
			{
				FolderStatistics stats;

				stats.target = _stats.direct.target;
				stats.size = _stats.direct.read;
				stats.blocks = _stats.direct.blocks;
				stats.files = _stats.direct.items;

				auto b_size = bundle_size("|||Backup Statistics|||", sizeof(FolderStatistics));

				auto [queue, off] = current.Incidental(b_size);

				*(uint32_t*)(queue) = (uint32_t)b_size;

				current.Insert(domain, off);

				//There used to be a time stamp in this object. This however caused deduplication of metadata to always fail.
				//NO TIME STAMPS.
				//

				stream(queue, b_size, "|||Backup Statistics|||", 0, 0, gsl::span<uint8_t>((uint8_t*)&stats,sizeof(FolderStatistics)));
			}

			void OpenForWriting()
			{
				d8u::util::empty_file(string(root) + "/lock.db");
			}

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

				std::filesystem::remove(string(root) + "/lock.db");

				return string(root) + "/latest.db";
			}

			bool Excluded(std::string_view s)
			{
				auto path = exclude("path");
				auto file = exclude("file")[s];

				if (file)
					return true;

				bool exclude = false;

				path.ForEachValue([&](auto key, auto value) 
				{
					if (s.size() >= key.size())
					{
						if(std::equal((char*)key.data(),(char*)key.data()+key.size(),s.begin(),s.begin() + key.size()))
							exclude = true;
					}
				});

				return exclude;
			}

			uint8_t* Queue(std::string_view s, uint64_t size, uint64_t when, uint64_t BLOCK, uint64_t MAX)
			{
				if (Excluded(s))
					return nullptr;

				auto key_payload = (size > MAX) ? 32 : 32 * (size / BLOCK + 1 /*FILE HASH*/ + ((size % BLOCK) ? 1 : 0));

				auto b_size = bundle_size(s, key_payload);
				auto [queue, off] = current.Incidental(b_size);

				*(uint32_t*)(queue) = (uint32_t)b_size;

				current.Insert(s, off);

				return queue;
			}

			bool Changed(std::string_view s, uint64_t size, uint64_t when, uint8_t * queue )
			{
				if (!queue)
				{
					auto p = change.Find(s);
					return p ? !(*p == when) : true;
				}

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

				span<TH> data((TH*)(p + 24+ns), ds / sizeof(TH));

				return std::make_tuple(size, time, name, data);
			}

			static auto DecodeRaw(uint8_t* p)
			{
				uint32_t extent = *(uint32_t*)p;
				uint64_t size = *(uint64_t*)(p + 4);
				uint64_t time = *(uint64_t*)(p + 12);
				uint16_t ns = *(uint16_t*)(p + 20);

				std::string_view name((char*)(p + 22), ns);

				uint16_t ds = *(uint16_t*)(p + 22 + ns);

				span<uint8_t> data((p + 24 + ns), ds);

				return std::make_tuple(size, time, name, data);
			}

		private:

			size_t bundle_size(std::string_view s, size_t k_size)
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