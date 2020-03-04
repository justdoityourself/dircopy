/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "d8u/transform.hpp"
#include "backup.hpp"
#include "restore.hpp"
#include "delta.hpp"

#include "d8u/util.hpp"

#include "tdb/database.hpp"

namespace dircopy
{
	namespace validate
	{
		using namespace d8u::transform;
		using namespace d8u::util;

		//Shallow validation can happen on the server side:
		//We only ever send the ID to the store.
		//

		template <typename S, typename D> bool block(Statistics& stats, const DefaultHash& key, const S& store, const D& domain /*unused, API compatibility only*/)
		{
			auto id = key.Next();

			stats.atomic.blocks++;

			return store.Validate(id, validate_block<vector<uint8_t>>);
		}

		//Deep validation requires the Key, which should never be shared with the storage server:
		//Therefore we read the block instead of sending the key over the wire.
		//

		template <typename S, typename D> bool deep_block(Statistics & stats,const DefaultHash& key, const S& store, const D& domain)
		{
			try
			{
				auto file_id = key.Next();
				auto block = store.Read(file_id);

				stats.atomic.blocks++;
				stats.atomic.read += block.size();

				decode(domain, block, key);

				DefaultHash dup_key(domain, block);

				if (!std::equal(key.begin(), key.end(), dup_key.begin()))
					return false;

				return true;
			}
			catch (...) {}

			return false;
		}

		//TODO add parallel, see restore::file for template
		template <typename S, typename D, typename V> bool core_file(Statistics& stats, const DefaultHash& file_key, const S& store, const D& domain, V v, size_t P = 1)
		{
			try
			{
				std::atomic<size_t> local = 0;

				auto file_id = file_key.Next();
				auto file_record = store.Read(file_id);

				stats.atomic.blocks++;
				stats.atomic.read += file_record.size();

				decode(domain, file_record, file_key);

				DefaultHash dup_key(domain, file_record);

				if (!std::equal(file_key.begin(), file_key.end(), dup_key.begin()))
					return false;

				if (file_record.size() % sizeof(DefaultHash) != 0)
					return false;

				auto count = file_record.size() / sizeof(DefaultHash);

				bool result = true;

				if (P == 1)
				{
					for (size_t i = 0; i < count - 1 /*Last hash is the file hash*/; i++)
					{
						auto key = ((DefaultHash*)file_record.data()) + i;

						if (!v(stats, *key, store, domain))
							return false;
					}
				}
				else
				{
					for (size_t i = 0; result && i < count - 1 /*Last hash is the file hash*/; i++)
					{
						auto key = ((DefaultHash*)file_record.data()) + i;

						fast_wait(stats.atomic.threads, P);

						stats.atomic.threads++;
						local++;

						std::thread([&](DefaultHash* bk)
						{
							if (!v(stats, *bk, store, domain))
								result = false;

							stats.atomic.threads--;
							local--;
						}, key ).detach();
					}

					slow_wait(local);
				}

				return result;
			}
			catch (...) {}

			return false;
		}

		template <typename S, typename D> std::pair<bool, Direct> file(const DefaultHash& file_key, const S& store, const D& domain, size_t P = 1)
		{
			Statistics s;

			return std::make_pair(core_file(s,file_key, store, domain, block<S, D>,P),s.direct);
		}

		template <typename S, typename D> std::pair<bool, Direct> deep_file(const DefaultHash& file_key, const S& store, const D& domain, size_t P = 1)
		{
			Statistics s;

			return std::make_pair(core_file(s,file_key, store, domain, deep_block<S, D>,P),s.direct);
		}

		//todo parallel, see restore::folder
		template <typename S, typename D, typename V> bool core_folder(Statistics &s ,const DefaultHash& folder_key, const S& store, const D& domain, V v, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			try
			{
				std::atomic<size_t> files = 0;

				auto folder_id = folder_key.Next();
				auto folder_record = store.Read(folder_id);

				decode(domain, folder_record, folder_key);

				DefaultHash dup_key(domain, folder_record);

				if (!std::equal(folder_key.begin(), folder_key.end(), dup_key.begin()))
					return false;

				auto database = restore::file_memory(s, gsl::span<DefaultHash>((DefaultHash*)folder_record.data(),folder_record.size()/sizeof(DefaultHash)), store, domain, true, true);

				tdb::MemoryHashmap db(database);
				bool res = true;

				auto file = [&](uint64_t p)
				{
					dec_scope lock(files);

					auto [size, time, name, keys] = delta::Path::Decode(db.GetObject(p));

					if (keys.size() == 1)
					{
						/*if (keys.size() != 1)
							return res = false;*/

						if (!core_file(s, *keys.data(), store, domain, v,P))
							return res = false;
					}
					else
					{
						if (keys.size() <= 1)
							return res = false;

						for (auto& k : keys)
						{
							if (&k == keys.end() - 1)
								break; //Last hash is the file hash

							if (!v(s, k, store, domain))
								return res = false;
						}
					}

					return res;
				};

				if(F == 1)
					db.Iterate([&](uint64_t p)
					{
						return file(p);
					});
				else
				{
					db.Iterate([&](uint64_t p)
					{
						fast_wait(files, F);
							
						files++;

						std::thread(file, p).detach();

						return res;
					});

					slow_wait(files);
				}

				return res;
			}
			catch (...) {}

			return false;
		}

		template <typename S, typename D> std::pair<bool, Direct> folder(const DefaultHash& folder_key, const S& store, const D& domain, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			Statistics s;

			return std::make_pair(core_folder(s,folder_key, store, domain, block<S, D>, BLOCK, THRESHOLD,P,F),s.direct);
		}

		template <typename S, typename D> std::pair<bool, Direct> deep_folder(const DefaultHash& folder_key, const S& store, const D& domain, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			Statistics s;

			return std::make_pair(core_folder(s,folder_key, store, domain, deep_block<S, D>, BLOCK, THRESHOLD, P, F),s.direct);
		}
	}
}