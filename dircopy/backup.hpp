/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once


#ifdef _WIN32

#include "volsnap/snapshot.hpp"

#endif

#include <thread>
#include <future>
#include <list>
#include <tuple>
#include <vector>
#include <string>
#include <filesystem>
#include <bitset>

#include "../mio.hpp"
#include "../gsl-lite.hpp"

#include "d8u/transform.hpp"
#include "defs.hpp"

#include "d8u/util.hpp"

#include "delta.hpp"

using gsl::span;

namespace dircopy
{
	namespace backup
	{
		using namespace d8u::transform;
		using namespace defs;
		using namespace d8u::util;

		template < typename STORE, typename D > DefaultHash block(Statistics& stats, std::vector<uint8_t>& buffer, STORE& store, const D& domain = default_domain, int compression = 5)
		{
			stats.atomic.read += buffer.size(); stats.atomic.blocks++;

			DefaultHash key, id;
			std::tie(key, id) = identify(domain, buffer);

			if (store.Is(id))
			{
				stats.atomic.duplicate += buffer.size();
				stats.atomic.dblocks++;
				return key;
			}

			encode2(buffer, key, id, compression);

			store.Write(id, buffer);
			stats.atomic.write += buffer.size();

			return key;
		}

		/*template < typename STORE, typename D > DefaultHash block_span(Statistics& stats, gsl::span<uint8_t> span, STORE& store, const D& domain = default_domain, int compression = 5)
		{
			stats.atomic.read += span.size(); stats.atomic.blocks++;

			DefaultHash key, id;
			std::tie(key, id) = identify(domain, span);

			if (store.Is(id))
			{
				stats.atomic.duplicate += span.size();
				stats.atomic.dblocks++;
				return key;
			}

			auto [_key,buffer] = encode2_span(span, key, id, compression);

			store.Write(id, buffer);
			stats.atomic.write += buffer.size();

			return key;
		}*/

		template < typename STORE, typename D > void async_block(size_t MAX, DefaultHash& out, Statistics& stats, std::vector<uint8_t> buffer, STORE& store, const D& domain = default_domain, int c = 5, std::atomic<size_t>* plocal = nullptr)
		{
			if (MAX == 0)
			{
				out = block(stats, buffer, store, domain, c);
				return;
			}

			while (stats.atomic.threads.load() >= MAX)
				std::this_thread::sleep_for(std::chrono::milliseconds(10));

			stats.atomic.threads++;

			std::thread([c,&domain = domain, &store = store, &out = out, &stats = stats, plocal](std::vector<uint8_t> buffer) mutable
			{
				out = block(stats, buffer, store, domain, c);
				stats.atomic.threads--;

				if (plocal) (*plocal)+=1;
			}, std::move(buffer)).detach();
		}

		/*template < typename STORE, typename D > void async_block_span(size_t MAX, DefaultHash& out, Statistics& stats, gsl::span<uint8_t> buffer, STORE& store, const D& domain = default_domain, int c = 5, std::atomic<size_t>* plocal = nullptr)
		{
			if (MAX == 0)
			{
				out = block_span(stats, buffer, store, domain, c);
				return;
			}

			while (stats.atomic.threads.load() >= MAX)
				std::this_thread::sleep_for(std::chrono::milliseconds(10));

			stats.atomic.threads++;

			std::thread([buffer,c,&domain = domain, &store = store, &out = out, &stats = stats, plocal]() mutable
			{
				out = block_span(stats, buffer, store, domain, c);
				stats.atomic.threads--;

				if (plocal) *plocal++;
			}).detach();
		}*/

		template < typename MAP, typename STORE, typename D > std::vector<uint8_t> core_file(Statistics& stats, const MAP& file, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			std::atomic<int> local_threads = 0;

			HashState hash_state;
			hash_state.Update(domain); //Protect against content queries.

			std::vector<uint8_t> result;

			//Keep block ids in memory to construct the file handle block:
			//

			auto count = file.size() / BLOCK + ((file.size() % BLOCK) ? 1 : 0);
			result.reserve(sizeof(DefaultHash) * (count + 1)); // + File Hash


			//Store unique block:
			//

			auto save = [&](auto slice, auto key, auto id)
			{
				std::vector<uint8_t> buffer(slice.size());
				std::copy(slice.begin(), slice.end(), buffer.begin());

				encode2(buffer, key, id, compression);

				store.Write(id, buffer);
				stats.atomic.write += buffer.size();

				if (THREADS != 1)
				{
					stats.atomic.threads--;
					local_threads--;
				}
			};


			//Grouping to optimized remote queries:
			//

			std::vector<std::pair<DefaultHash, span<const uint8_t>>> group; 		group.reserve(GROUP);
			std::vector<DefaultHash> ids;											ids.reserve(GROUP);

			auto push_group = [&]()
			{
				uint64_t _bitmap;

				if (ids.size() > 1)
					_bitmap = store.Many<sizeof(DefaultHash)>(span<uint8_t>((uint8_t*)ids.data(), ids.size() * sizeof(DefaultHash)));
				else
					_bitmap = (uint64_t)store.Is(*ids.begin());

				auto bitmap = std::bitset<64>(_bitmap);

				for (size_t i = 0; i < group.size(); i++)
				{
					if (bitmap[i])
					{
						stats.atomic.duplicate += group[i].second.size();
						stats.atomic.dblocks++;
						continue;
					}

					if (THREADS == 1)
						save(group[i].second, group[i].first, ids[i]);
					else
					{
						while (stats.atomic.threads.load() >= THREADS) //This will not be exact if multiple files are processed at the same time, a few more threads will be run than the limit. Use CMP_EXCHANGE. Worse case this causes overthrottling.
							std::this_thread::sleep_for(std::chrono::milliseconds(10)); // SHOULD be the time it takes for one block to be encoded.

						stats.atomic.threads++;
						local_threads++;

						std::thread(save, group[i].second, group[i].first, ids[i]).detach();
					}
				}

				group.clear();
				ids.clear();
			};


			//Map, id and backup one block at a time:
			//

			for (size_t i = 0; i < file.size(); i += BLOCK)
			{
				auto rem = file.size() - i;
				auto cur = BLOCK;

				if (rem < cur) cur = rem;


				//Map data and iterate file hash:
				//

				span<const uint8_t> seg((const uint8_t*)file.data() + i, cur);
				stats.atomic.read += cur; stats.atomic.blocks++;
				hash_state.Update(seg);


				//Identify as unique:
				//

				DefaultHash key, id; std::tie(key, id) = identify(domain, seg);
				result.insert(result.end(), key.begin(), key.end());

				if (GROUP == 1)
				{
					if (store.Is(id))
					{
						stats.atomic.duplicate += cur;
						stats.atomic.dblocks++;
						continue;
					}

					if (THREADS == 1)
						save(seg,key,id);
					else
					{
						while (stats.atomic.threads.load() >= THREADS) //This will not be exact if multiple files are processed at the same time, a few more threads will be run than the limit. Use CMP_EXCHANGE. Worse case this causes overthrottling.
							std::this_thread::sleep_for(std::chrono::milliseconds(10)); // SHOULD be the time it takes for one block to be encoded.

						stats.atomic.threads++;
						local_threads++;

						std::thread(save, seg,key,id).detach();
					}
				}
				else
				{ 
					ids.push_back(id);

					group.push_back(std::make_pair(key, seg));

					if (group.size() == GROUP)
						push_group();
				}
			}

			if (GROUP != 1 && ids.size())
			{
				//Flush Group:
				//

				push_group();
			}


			auto file_hash = hash_state.Finish();
			result.insert(result.end(), file_hash.begin(), file_hash.end());

			//Wait for threads to close before return:
			//

			while (THREADS != 1 && local_threads.load())
				std::this_thread::sleep_for(std::chrono::milliseconds(10));


			return result;
		}


		template < typename MAP, typename STORE, typename D> DefaultHash submit_core(Statistics& stats, const MAP& file, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			DefaultHash key, id;

			//Store file blocks:
			//

			auto result = core_file(stats, file, store, domain, BLOCK, THREADS, compression, GROUP);


			//Coalesce ids into single file handle and write it to store:
			//Identify as unique:
			//

			std::tie(key, id) = identify(domain, result);

			if (store.Is(id)) //TODO METADATA is not reported on this function
				return key;

			//Write unique block:
			//

			encode2(result, key, id,compression);

			store.Write(id, result); //Files larger than 32 GB exceed 1MB of metadata, breaking the unenforced limit.

			return key;
		}


		template < typename STORE, typename D > BlockResult single_file(std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			BlockResult stats;

			if (std::filesystem::file_size(name) == 0)
				return stats;

			mio::mmap_source file(name);

			stats.key_list = core_file<STORE, D, BLOCK, THREADS>(stats, file, store, domain, BLOCK, THREADS, compression, GROUP);

			return stats;
		}


		template < typename STORE, typename D> KeyResult submit_file(std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			Statistics stats;

			if (std::filesystem::file_size(name) == 0)
				return { DefaultHash(), stats.direct };

			mio::mmap_source file(name);

			auto key = submit_core(stats, file, store, domain, BLOCK, THREADS, compression, GROUP);

			return { key, stats.direct };
		}

		template < typename STORE, typename D > std::vector<uint8_t> single_file2(Statistics& stats, std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			if (std::filesystem::file_size(name) == 0)
				return std::vector<uint8_t>();

			mio::mmap_source file(name);

			return core_file(stats, file, store, domain, BLOCK, THREADS, compression, GROUP);
		}


		template < typename STORE, typename D> DefaultHash submit_file2(Statistics& stats, std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			if (std::filesystem::file_size(name) == 0)
				return DefaultHash();

			mio::mmap_source file(name);

			return submit_core(stats, file, store, domain, BLOCK, THREADS, compression, GROUP);
		}

		template < typename DITR, typename ON_FILE > uint64_t core_delta(delta::Path& db, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel_count = 0)
		{
			uint64_t total_size = 0;

			for (auto& e : DITR(path, std::filesystem::directory_options::skip_permission_denied))
			{
				if (e.is_directory())
					continue;

				uint64_t change_time = GetFileWriteTime(e);
				auto rel = e.path().string();
				auto full = rel;

				if (rel_count)
				{
					//When using a snapshot or mountpoint, correct the path:
					//

					for (int c = 0, i = 0; i < rel.size(); i++)
					{
						if (rel[i] == '/' || rel[i] == '\\')
						{
							c++;

							if (rel_count == c)
							{
								rel = std::string(drive) + rel.substr(i + 1);
								break;
							}
						}
					}
				}

				if (db.Excluded(rel))
					continue;

				uint64_t size = e.file_size();
				total_size += size;

				if (!db.Changed(rel, size, change_time, nullptr))
					continue;

				if (!on_file(rel, size, change_time))
					break;
			}

			return total_size;
		}

		template < typename DITR, typename STORE, typename ON_FILE, typename D > void core_folder(delta::Path & db, Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel_count = 0)
		{
			for (auto& e : DITR(path,std::filesystem::directory_options::skip_permission_denied))
			{
				if (e.is_directory())
					continue;

				stats.atomic.items++;

				uint64_t change_time = GetFileWriteTime(e);
				auto full = e.path().string();
				auto rel = full.substr(path.size());


				uint64_t size = e.file_size();

				auto queue = db.Queue(rel, size, change_time, BLOCK, LARGE_THRESHOLD);

				if (!queue) //Excluded
					return;

				if (!db.Changed(rel, size, change_time, queue))
				{
					stats.atomic.read += size;
					stats.atomic.blocks += (size / BLOCK +((size%BLOCK) ? 1 : 0) /*+ ((size >= LARGE_THRESHOLD) ? 1 : 0) ... use this when the large file metadata is fixed*/);
					continue;
				}

				auto _file = [&](std::string handle, std::string name, uint64_t size, uint64_t changed, uint8_t* queue)
				{
					if (size >= LARGE_THRESHOLD)
					{
						auto key = submit_file2(stats, handle, store, domain, BLOCK, THREADS, compression, GROUP);

						db.Apply(name,size, changed,key,queue);
					}
					else
					{
						auto block = single_file2(stats, handle, store, domain, BLOCK, THREADS, compression, GROUP);

						db.Apply(name, size, changed, block,queue);
					}

					if (FILES != 1)
						stats.atomic.files--;
				};

				if (!on_file(rel, size, change_time))
					break;

				if (FILES == 1)
					_file(full, rel, size, change_time, queue);
				else
				{
					while (stats.atomic.files.load() >= FILES)
						std::this_thread::sleep_for(std::chrono::milliseconds(20));

					stats.atomic.files++;

					std::thread(_file, full, rel, size, change_time, queue).detach();
				}
			}

			while (FILES != 1 && stats.atomic.files.load())
				std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}

		template < typename DITR, typename STORE, typename ON_FILE, typename D > DefaultHash submit_folder(std::string_view exclude, std::string_view delta_folder,Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0)
		{
			delta::Path db(delta_folder,exclude);

			core_folder<DITR>(db,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel);

			db.Statistics(stats,domain);

			return submit_file2(stats, db.Finalize(), store, domain, BLOCK, THREADS, compression, GROUP);
		}

		template < typename DITR, typename ON_FILE > uint64_t delta_folder(std::string_view exclude, std::string_view snapshot, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel = 0)
		{
			delta::Path db(snapshot,exclude);

			return core_delta<DITR>( db, path, on_file, drive, rel);
		}

		template < typename ON_FILE > uint64_t single_delta(std::string_view exclude, std::string_view snapshot, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel = 0)
		{
			return delta_folder< std::filesystem::directory_iterator >(exclude, snapshot, path, on_file, drive, rel);
		}

		template < typename ON_FILE> uint64_t recursive_delta(std::string_view exclude, std::string_view snapshot, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel = 0)
		{
			return delta_folder< std::filesystem::recursive_directory_iterator >(exclude, snapshot, path, on_file, drive, rel);
		}

		template < typename STORE, typename ON_FILE, typename D > KeyResult single_folder(std::string_view exclude, std::string_view delta_folder,std::string_view path, STORE& store, ON_FILE &&on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0)
		{
			Statistics stats;

			auto key = submit_folder< std::filesystem::directory_iterator>(exclude, delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel);

			return { key, stats.direct };
		}


		template < typename STORE, typename ON_FILE, typename D > KeyResult recursive_folder(std::string_view exclude, std::string_view delta_folder, std::string_view path, STORE& store, ON_FILE &&on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0)
		{
			Statistics stats;

			auto key = submit_folder< std::filesystem::recursive_directory_iterator>(exclude, delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel);

			return { key, stats.direct };
		}

		template < typename STORE, typename ON_FILE, typename D > DefaultHash single_folder2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0)
		{
			return submit_folder< std::filesystem::directory_iterator>(exclude,delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel);
		}

		template < typename STORE, typename ON_FILE, typename D > DefaultHash recursive_folder2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive ="",size_t rel = 0)
		{
			return submit_folder< std::filesystem::recursive_directory_iterator>(exclude, delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel);
		}



#ifdef _WIN32

		template < typename STORE, typename ON_FILE, typename D > DefaultHash vss_folder2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view _path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024)
		{
			std::string full = std::filesystem::absolute(_path).string();

			bool use_root = false;// full.size() != _path.size();

			std::string path = full.substr(2);
			std::string drive = full.substr(0,1) + ":\\";

			volsnap::win32::Snapshot sn(drive);

			auto root = sn.Mount(std::filesystem::absolute(delta_folder).string() + "\\mount");

			std::string vss_path; vss_path += root; vss_path += path;

			size_t rel = std::count(vss_path.begin(), vss_path.end(), '\\') + std::count(vss_path.begin(), vss_path.end(), '/');

			auto r = recursive_folder2(exclude, delta_folder,stats, vss_path,store,on_file,domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,(use_root) ? drive : "",rel);

			sn.Dismount();

			return r;
		}

		template < typename STORE, typename ON_FILE, typename D > KeyResult vss_folder(std::string_view exclude, std::string_view delta_folder, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024)
		{
			Statistics stats;

			auto r = vss_folder2(exclude, delta_folder, stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD);

			return { r, stats.direct };
		}

		template < typename STORE, typename ON_FILE, typename D > DefaultHash vss_single2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view _path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024)
		{
			std::string full = std::filesystem::absolute(_path).string();

			bool use_root = false;// full.size() != _path.size();

			std::string path = full.substr(2);
			std::string drive = full.substr(0, 1) + ":\\";

			volsnap::win32::Snapshot sn(drive);

			auto root = sn.Mount(std::filesystem::absolute(delta_folder).string() + "\\mount");

			std::string vss_path; vss_path += root; vss_path += path;

			size_t rel = std::count(vss_path.begin(), vss_path.end(), '\\') + std::count(vss_path.begin(), vss_path.end(), '/');

			auto r = single_folder2(exclude, delta_folder, stats, vss_path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD, (use_root) ? drive : "", rel);

			sn.Dismount();

			return r;
		}

		template < typename STORE, typename ON_FILE, typename D > KeyResult vss_single(std::string_view exclude, std::string_view delta_folder, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024)
		{
			Statistics stats;

			auto r = vss_single2(exclude, delta_folder, stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD);

			return { r, stats.direct };
		}

#endif
	}
}