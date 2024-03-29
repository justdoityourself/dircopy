/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "d8u/transform.hpp"
#include "backup.hpp"
#include "restore.hpp"
#include "delta.hpp"

#include "d8u/util.hpp"

#include "tdb/legacy.hpp"

namespace dircopy
{
	namespace validate
	{
		using namespace d8u::transform;
		using namespace d8u::util;

		//Shallow validation can happen on the server side:
		//We only ever send the ID to the store.
		//

		template <typename TH, typename S, typename D> bool block(Statistics& stats, TH key, S& store, const D& domain /*unused, API compatibility only*/)
		{
			auto id = key.GetNext();

			stats.atomic.blocks++;

			return store.Validate(id, validate_block<TH, d8u::sse_vector>);
		}

		//Deep validation requires the Key, which should never be shared with the storage server:
		//Therefore we read the block instead of sending the key over the wire.
		//

		template <typename TH, typename S, typename D> bool deep_block(Statistics & stats,TH key, S& store, const D& domain)
		{
			try
			{
				auto file_id = key.GetNext();
				auto block = store.Read(file_id);

				stats.atomic.blocks++;
				stats.atomic.write += block.size();

				decode(domain, block, key);

				TH dup_key(domain, block);

				if (!std::equal(key.begin(), key.end(), dup_key.begin()))
					return false;

				return true;
			}
			catch (...) {}

			return false;
		}

		//TODO add parallel, see restore::file for template
		template <typename TH, typename S, typename D, typename V> bool core_file(Statistics& stats, TH file_key, S& store, const D& domain, V v, size_t P = 1)
		{
			try
			{
				std::atomic<size_t> local = 0;

				auto file_id = file_key.GetNext();
				auto file_record = store.Read(file_id);

				stats.atomic.blocks++;
				stats.atomic.write += file_record.size();

				decode(domain, file_record, file_key);

				TH dup_key(domain, file_record);

				if (!std::equal(file_key.begin(), file_key.end(), dup_key.begin()))
					return false;

				if (file_record.size() % sizeof(TH) != 0)
					return false;

				auto count = file_record.size() / sizeof(TH);

				bool result = true;

				if (P == 1)
				{
					for (size_t i = 0; i < count - 1 /*Last hash is the file hash*/; i++)
					{
						auto key = ((TH*)file_record.data()) + i;

						if (!v(stats, *key, store, domain))
							return false;
					}
				}
				else
				{
					for (size_t i = 0; result && i < count - 1 /*Last hash is the file hash*/; i++)
					{
						auto key = ((TH*)file_record.data()) + i;

						fast_wait(stats.atomic.threads, P);

						stats.atomic.threads++;
						local++;

						std::thread([&](TH* bk)
						{
							if (!v(stats, *bk, store, domain))
								result = false;

							stats.atomic.threads--;
							local--;
						}, key ).detach();
					}

					fast_wait(local);
				}

				return result;
			}
			catch (...) {}

			return false;
		}

		template <typename TH, typename S, typename D> std::pair<bool, Direct> file(const TH& file_key, S& store, const D& domain, size_t P = 1)
		{
			Statistics s;

			return std::make_pair(core_file(s,file_key, store, domain, block<S, D>,P),s.direct);
		}

		template <typename TH, typename S, typename D> std::pair<bool, Direct> deep_file(const TH& file_key, S& store, const D& domain, size_t P = 1)
		{
			Statistics s;

			return std::make_pair(core_file(s,file_key, store, domain, deep_block<S, D>,P),s.direct);
		}

		//todo parallel, see restore::folder
		template <typename TH, typename S, typename D, typename V> bool core_folder(Statistics &s ,TH folder_key, S& store, const D& domain, V v, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			try
			{
				auto folder_id = folder_key.GetNext();
				auto folder_record = store.Read(folder_id);

				if (!validate_block<TH, d8u::sse_vector>(folder_record))
					return false;

				decode(domain, folder_record, folder_key);

				TH dup_key(domain, folder_record);

				if (!std::equal(folder_key.begin(), folder_key.end(), dup_key.begin()))
					return false;

				auto database = restore::file_memory(s, gsl::span<TH>((TH*)folder_record.data(),folder_record.size()/sizeof(TH)), store, domain, true, true);

				tdb::MemoryHashmap db(std::move(database));
				bool res = true;

				{
					auto [size, time, name, data] = delta::Path<TH>::DecodeRaw(db.FindObject(domain));

					auto stats = (typename delta::Path<TH>::FolderStatistics*)data.data();

					s.direct.target = stats->size;
				}

				auto file = [&](uint64_t p)
				{
					dec_scope lock(s.atomic.files);

					auto [size, time, name, keys] = delta::Path<TH>::Decode(db.GetObject(p));

					if (!size)
						return res;

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

					s.atomic.read += size;

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
						fast_wait(s.atomic.files, F);
							
						s.atomic.files++;

						std::thread(file, p).detach();

						return res;
					});

					fast_wait(s.atomic.files);
				}

				return res;
			}
			catch (...) {}

			return false;
		}

		template <typename TH, typename S, typename D> bool folder2(Statistics &s,const TH& folder_key, S& store, const D& domain, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			return core_folder<TH>(s, folder_key, store, domain, block<TH,S, D>, BLOCK, THRESHOLD, P, F);
		}

		template <typename TH, typename S, typename D> bool deep_folder2(Statistics& s, const TH& folder_key, S& store, const D& domain, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			return core_folder<TH>(s, folder_key, store, domain, deep_block<TH,S, D>, BLOCK, THRESHOLD, P, F);
		}

		template <typename TH, typename S, typename D> std::pair<bool, Direct> folder(const TH& folder_key, S& store, const D& domain, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			Statistics s;

			return std::make_pair(core_folder<TH>(s,folder_key, store, domain, block<TH,S, D>, BLOCK, THRESHOLD,P,F),s.direct);
		}

		template <typename TH, typename S, typename D> std::pair<bool, Direct> deep_folder(const TH& folder_key, S& store, const D& domain, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			Statistics s;

			return std::make_pair(core_folder<TH>(s,folder_key, store, domain, deep_block<TH,S, D>, BLOCK, THRESHOLD, P, F),s.direct);
		}
	}
}