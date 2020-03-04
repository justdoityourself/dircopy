/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "d8u/util.hpp"

#include "restore.hpp"
#include "delta.hpp"

namespace dircopy
{
	namespace mount
	{
		using namespace d8u::util;

		template < typename S, typename D > class Path
		{
			S & store;
			D & domain;
			Statistics stats;

			tdb::MemoryHashmap db;

			size_t BLOCK;
			size_t THRESHOLD;

			bool validate;

		public:
			Path(const DefaultHash& folder_key, S & _s, D& _d, size_t B = 1024 * 1024, size_t T = 128 * 1024 * 1024, bool v = true)
				: BLOCK(B)
				, THREASHOLD(T)
				, store(_s)
				, domain(_d)
				, validate(v)
			{
				auto folder_record = restore::block(stats, folder_key, store, domain, validate);

				auto database = restore::file_memory(stats, gsl::span<DefaultHash>((DefaultHash*)folder_record.data(), folder_record.size() / sizeof(DefaultHash)), store, domain, validate, validate);

				db = tdb::MemoryHashmap(database);
			}

			auto Usage()
			{
				return stats.direct;
			}

			//true continues enumeration, false ends it. keys are used to fetch file blocks
			//bool(uint64_t size, uint64_t last_write, string_view name, span keys)
			template < typename F > Enumerate(F&& f)
			{
				db.Iterate([&, f = std::move(f)](uint64_t p)
				{
					auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(p));

					return f(size,time,name,keys);
				});
			}

			template < typename F > Search(std::string_view target, F&& f)
			{
				db.Iterate([&, f = std::move(f)](uint64_t p)
				{
					auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(p));

					auto it = std::search(name.begin(), name.end(), target.begin(), target.end(), [](char ch1, char ch2) { return std::tolower(ch1) == std::tolower(ch2); });

					if (it != name.end())
						return f(size, time, name, keys);

					return true;
				});
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
					temp = restore::block(s, *keys.data(), store, domain, P))
					keys = gsl::span<DefaultHash>((*DefaultHash)temp.date(), temp.size() / sizeof(DefaultHash));
				}
	
				return restore::file_memory(stats, keys, store, domain, validate, validate);

			}

			void Download(std::string_view name, std::string_view dest, size_t P = 4)
			{
				std::vector<uint8_t> temp;
				auto p = db.Find(name);

				if (!p)
					return false;

				auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(*p));

				if (keys.size() == 1)
				{
					temp = restore::block(s, *keys.data(), store, domain, P))
					keys = gsl::span<DefaultHash>((*DefaultHash)temp.date(), temp.size() / sizeof(DefaultHash));
				}

				restore::_file2(stats, dest,keys, store, domain, validate, validate, P);
			}
		};
	}
}