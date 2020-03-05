/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "d8u/util.hpp"
#include "d8u/transform.hpp"

#include "../gsl-lite.hpp"

#include "restore.hpp"
#include "delta.hpp"

namespace dircopy
{
	namespace mount
	{
		using namespace d8u::util;
		using namespace d8u::transform;

		template < typename S, typename D > class Path
		{
			S & store;
			D & domain;
			Statistics stats;

			tdb::MemoryHashmap db;

			bool validate;

		public:
			Path(const DefaultHash & folder_key, S & _s, D& _d, bool v = true)
				: store(_s)
				, domain(_d)
				, validate(v)
			{
				auto folder_record = restore::block(stats, folder_key, store, domain, validate);

				auto database = restore::file_memory(stats, gsl::span<DefaultHash>((DefaultHash*)folder_record.data(), folder_record.size() / sizeof(DefaultHash)), store, domain, validate, validate);

				db.Open(database);
			}

			auto Usage()
			{
				return stats.direct;
			}

			void PrintUsage()
			{
				stats.Print();
			}

			//true continues enumeration, false ends it. keys are used to fetch file blocks
			//bool(uint64_t size, uint64_t last_write, string_view name, span keys)
			template < typename F > size_t Enumerate(F&& f)
			{
				return (size_t) db.Iterate([&, f = std::move(f)](uint64_t p)
				{
					auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(p));

					return f(size,time,name,keys);
				});
			}

			template < typename F > size_t Search(std::string_view target, F&& f)
			{
				size_t total = 0;

				db.Iterate([&, f = std::move(f)](uint64_t p)
				{
					auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(p));

					auto it = std::search(name.begin(), name.end(), target.begin(), target.end(), [](char ch1, char ch2) { return std::tolower(ch1) == std::tolower(ch2); });

					if (it != name.end())
					{
						total++;
						return f(size, time, name, keys);
					}

					return true;
				});

				return total;
			}

			std::vector<uint8_t> Memory(std::string_view name, size_t P = 4)
			{
				std::vector<uint8_t> temp;
				auto p = db.Find(name);

				if (!p)
					return false;
				
				auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(*p));

				if (keys.size() == 1)
				{
					temp = restore::block(stats, *keys.data(), store, domain, P);
					keys = gsl::span<DefaultHash>((DefaultHash*)temp.data(), temp.size() / sizeof(DefaultHash));
				}
	
				return restore::file_memory(stats, keys, store, domain, validate, validate);
			}

			void Fetch(std::string_view _name, std::string_view dest, size_t P = 4)
			{
				std::vector<uint8_t> temp;
				auto p = db.Find(_name);

				if (!p)
					return;

				auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(*p));

				if (keys.size() == 1)
				{
					temp = restore::block(stats, *keys.data(), store, domain, P);
					keys = gsl::span<DefaultHash>((DefaultHash*)temp.data(), temp.size() / sizeof(DefaultHash));
				}

				restore::_file2(stats, dest,keys, store, domain, validate, validate, P);
			}
		};
	}
}