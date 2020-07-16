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
#include <fstream>
#include <bitset>

#include "../mio.hpp"
#include "../gsl-lite.hpp"

#include "d8u/transform.hpp"
#include "defs.hpp"

#include "d8u/util.hpp"
#include "d8u/memory.hpp"

#include "delta.hpp"

using gsl::span;

namespace dircopy
{
	namespace backup
	{
		using namespace d8u::transform;
		using namespace defs;
		using namespace d8u::util;
		using namespace d8u;

		template < typename TH, typename STORE, typename D > TH block(Statistics& stats, sse_vector& buffer, STORE& store, const D& domain = default_domain, int compression = 5)
		{
			stats.atomic.read += buffer.size(); stats.atomic.blocks++;

			TH key, id;
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

		template < typename TH , typename STORE, typename D > void async_block(size_t MAX, TH& out, Statistics& stats, sse_vector buffer, STORE& store, const D& domain = default_domain, int c = 5, std::atomic<size_t>* plocal = nullptr)
		{
			if (MAX == 0)
			{
				out = block(stats, buffer, store, domain, c);
				return;
			}

			while (stats.atomic.threads.load() >= MAX)
				std::this_thread::sleep_for(std::chrono::milliseconds(10));

			stats.atomic.threads++;

			std::thread([c,&domain = domain, &store = store, &out = out, &stats = stats, plocal](sse_vector buffer) mutable
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

		template < typename TH , typename MAP, typename STORE, typename D > sse_vector core_file_map(Statistics& stats, const MAP& file, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1,size_t MAX_MEMORY = 128*1024*1024, size_t sq = -1)
		{
			auto MAX_CONN = MAX_MEMORY / (1024 * 1024) * 2;

			std::atomic<int> local_threads = 0;

			typename TH::State hash_state;
			hash_state.Update(domain); //Protect against content queries.

			sse_vector result;

			//Keep block ids in memory to construct the file handle block:
			//

			auto count = file.size() / BLOCK + ((file.size() % BLOCK) ? 1 : 0);
			result.resize(sizeof(TH) * (count + 1)); // + File Hash

			gsl::span<TH> result_keys((TH*)result.data(), count + 1);


			//Store unique block:
			//

			auto save = [&](auto slice, size_t dx)
			{
				//Identify as unique:
				//

				TH key, id; std::tie(key, id) = identify<TH>(domain, slice);
				result_keys[dx] = key;

				stats.atomic.threads--;

				while (stats.atomic.connections.load() >= MAX_CONN)
					std::this_thread::sleep_for(std::chrono::milliseconds(10));

				stats.atomic.connections++;

				if (store.Is(id))
				{
					stats.atomic.connections--;

					//if (THREADS != 1)
					//	stats.atomic.threads--;

					stats.atomic.memory -= slice.size();

					stats.atomic.duplicate += slice.size();
					stats.atomic.dblocks++;
				}
				else
				{
					stats.atomic.connections--;

					while (stats.atomic.threads.load() >= THREADS) 
						std::this_thread::sleep_for(std::chrono::milliseconds(10)); 

					stats.atomic.threads++;

					sse_vector buffer(slice.size());
					std::copy(slice.begin(), slice.end(), buffer.begin());

					stats.atomic.memory-=slice.size();

					encode2(buffer, key, id, compression);

					//Allow the next thread to start encoding while we write this buffer
					//if (THREADS != 1)
						stats.atomic.threads--;

					while (stats.atomic.connections.load() >= MAX_CONN)
						std::this_thread::sleep_for(std::chrono::milliseconds(10));

					stats.atomic.connections++;

					store.Write(id, buffer);
					stats.atomic.write += buffer.size();

					stats.atomic.connections--;
				}

				//if (THREADS != 1)
					local_threads--;
			};


			//Map, id and backup one block at a time:
			//

			if (sq != -1)
			{
				auto cq = stats.atomic.sequence.load();
				auto dq = sq - cq;

				while (dq)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(10));

					cq = stats.atomic.sequence.load();
					dq = sq - cq;
				}
			}

			for (size_t i = 0,dx=0; i < file.size(); i += BLOCK, dx++)
			{
				auto rem = file.size() - i;
				auto cur = BLOCK;

				if (rem < cur) cur = rem;


				//Map data and iterate file hash:
				//

				while (stats.atomic.memory.load() >= MAX_MEMORY)
					std::this_thread::sleep_for(std::chrono::milliseconds(10));

				span<const uint8_t> seg((const uint8_t*)file.data() + i, cur);
				stats.atomic.read += cur; stats.atomic.blocks++; stats.atomic.memory += cur;
				hash_state.Update(seg); //This action causes a page fault loading the blocks into ram.

				if (THREADS == 1)
					save(seg,dx);
				else
				{
					while (stats.atomic.threads.load() >= THREADS) //This will not be exact if multiple files are processed at the same time, a few more threads will be run than the limit. Use CMP_EXCHANGE. Worse case this causes overthrottling.
						std::this_thread::sleep_for(std::chrono::milliseconds(10)); // SHOULD be the time it takes for one block to be encoded.

					stats.atomic.threads++;
					local_threads++;

					std::thread(save, seg, dx).detach();
				}
			}

			//Streaming IO is complete for this file, allow the next to start
			stats.atomic.sequence++;
			stats.atomic.files--;

			//Wait for threads to close before return:
			//

			while (THREADS != 1 && local_threads.load())
				std::this_thread::sleep_for(std::chrono::milliseconds(10));

			auto file_hash = hash_state.FinishT<TH>();
			result_keys[count] = file_hash;


			return result;
		}

		template < typename TH , typename STORE, typename D > sse_vector core_file_stream(Statistics& stats, string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t MAX_MEMORY = 128 * 1024 * 1024,size_t sq = -1)
		{
			auto MAX_CONN = MAX_MEMORY / (1024 * 1024) * 2;

			std::atomic<int> local_threads = 0;
			std::ifstream file(name, ios::binary);
			auto file_size = GetFileSize(name);

			typename TH::State hash_state;
			hash_state.Update(domain); //Protect against content queries.

			sse_vector result;
			std::vector<sse_vector> blocks;

			//Keep block ids in memory to construct the file handle block:
			//

			auto count = file_size / BLOCK + ((file_size % BLOCK) ? 1 : 0);
			blocks.resize(count);
			result.resize(sizeof(TH) * (count + 1)); // + File Hash

			gsl::span<TH> result_keys((TH*)result.data(), count + 1);


			//Store unique block:
			//

			auto save = [&](auto buf, size_t dx)
			{
				//Identify as unique:
				//

				TH key, id; std::tie(key, id) = identify<TH>(domain, buf);
				result_keys[dx] = key;

				//stats.atomic.threads--;


				//while (stats.atomic.connections.load() >= MAX_CONN)
				//	std::this_thread::sleep_for(std::chrono::milliseconds(10));

				stats.atomic.connections++;

				if (store.Is(id))
				{
					stats.atomic.connections--;


					stats.atomic.memory -= buf.size();

					stats.atomic.duplicate += buf.size();
					stats.atomic.dblocks++;
				}
				else
				{
					stats.atomic.connections--;


					//while (stats.atomic.threads.load() >= THREADS)
					//	std::this_thread::sleep_for(std::chrono::milliseconds(10));

					//stats.atomic.threads++;
					auto sz = buf.size();

					encode2<TH>(buf, key, id, compression);

					//stats.atomic.threads--;


					//while (stats.atomic.connections.load() >= MAX_CONN)
					//	std::this_thread::sleep_for(std::chrono::milliseconds(10));

					stats.atomic.connections++;

					store.Write(id, buf);
					stats.atomic.write += buf.size();
					stats.atomic.memory -= sz;

					stats.atomic.connections--;
				}

				stats.atomic.threads--;

				local_threads--;
			};


			//Map, id and backup one block at a time:
			//

			if (sq != -1)
			{
				size_t cq = stats.atomic.sequence.load();

				while (sq > cq)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(10));

					cq = stats.atomic.sequence.load();
				}
			}

			std::thread sequential_hash([&]()
			{
				size_t dx = 0;

				while (dx < count)
				{
					if (!blocks[dx].size())
					{
						std::this_thread::sleep_for(std::chrono::milliseconds(10));
						continue;
					}

					hash_state.Update(blocks[dx]);

					while (stats.atomic.threads.load() >= THREADS) 
						std::this_thread::sleep_for(std::chrono::milliseconds(10));

					stats.atomic.threads++;
					local_threads++;

					std::thread(save, std::move(blocks[dx]), dx).detach();

					dx++;
				}
			});

			for (size_t i = 0, dx = 0; i < file_size; i += BLOCK, dx++)
			{
				auto rem = file_size - i;
				auto cur = BLOCK;

				if (rem < cur) cur = rem;

				//Read data and iterate file hash:
				//

				while (stats.atomic.memory.load() >= MAX_MEMORY)
					std::this_thread::sleep_for(std::chrono::milliseconds(10));

				sse_vector buf(cur);
				file.read((char*)buf.data(), cur);
				blocks[dx] = std::move(buf);

				stats.atomic.read += cur; 
				stats.atomic.blocks++; 
				stats.atomic.memory += cur;
			}

			//Streaming IO is complete for this file, allow the next to start:
			//

			stats.atomic.sequence++;
			stats.atomic.files--;

			sequential_hash.join();

			//Wait for threads to close before return:
			//

			while (local_threads.load())
				std::this_thread::sleep_for(std::chrono::milliseconds(10));

			auto file_hash = hash_state.FinishT<TH>();
			result_keys[count] = file_hash;

			return result;
		}

		template < typename TH, typename MAP, typename STORE, typename D > sse_vector _legacy_core_file(Statistics& stats, const MAP& file, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1)
		{
			std::atomic<int> local_threads = 0;

			typename TH::State hash_state;
			hash_state.Update(domain); //Protect against content queries.

			sse_vector result;

			//Keep block ids in memory to construct the file handle block:
			//

			auto count = file.size() / BLOCK + ((file.size() % BLOCK) ? 1 : 0);
			result.reserve(sizeof(TH) * (count + 1)); // + File Hash


			//Store unique block:
			//

			auto save = [&](auto slice, auto key, auto id)
			{
				sse_vector buffer(slice.size());
				std::copy(slice.begin(), slice.end(), buffer.begin());

				encode2<TH>(buffer, key, id, compression);

				store.Write(id, buffer);
				stats.atomic.write += buffer.size();

				stats.atomic.threads--;
				local_threads--;
			};


			//Grouping to optimized remote queries:
			//

			std::vector<std::pair<TH, span<const uint8_t>>> group; 		group.reserve(GROUP);
			std::vector<TH> ids;											ids.reserve(GROUP);

			auto push_group = [&]()
			{
				uint64_t _bitmap;

				if (ids.size() > 1)
					_bitmap = store.Many<sizeof(TH)>(span<uint8_t>((uint8_t*)ids.data(), ids.size() * sizeof(TH)));
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

					while (stats.atomic.threads.load() >= THREADS) //This will not be exact if multiple files are processed at the same time, a few more threads will be run than the limit. Use CMP_EXCHANGE. Worse case this causes overthrottling.
						std::this_thread::sleep_for(std::chrono::milliseconds(10)); // SHOULD be the time it takes for one block to be encoded.

					stats.atomic.threads++;
					local_threads++;

					std::thread(save, group[i].second, group[i].first, ids[i]).detach();
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

				TH key, id; std::tie(key, id) = identify(domain, seg);
				result.insert(result.end(), key.begin(), key.end());

				if (GROUP == 1)
				{
					if (store.Is(id))
					{
						stats.atomic.duplicate += cur;
						stats.atomic.dblocks++;
						continue;
					}

					while (stats.atomic.threads.load() >= THREADS) //This will not be exact if multiple files are processed at the same time, a few more threads will be run than the limit. Use CMP_EXCHANGE. Worse case this causes overthrottling.
						std::this_thread::sleep_for(std::chrono::milliseconds(10)); // SHOULD be the time it takes for one block to be encoded.

					stats.atomic.threads++;
					local_threads++;

					std::thread(save, seg,key,id).detach();
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

			while (local_threads.load())
				std::this_thread::sleep_for(std::chrono::milliseconds(10));


			return result;
		}


		template < bool MMAP = true, typename TH, typename MAP, typename STORE, typename D> TH submit_core(Statistics& stats, const MAP& file, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1,size_t MAX_MEMORY = 128*1024*1024,size_t sq = -1)
		{
			TH key, id;

			//Store file blocks:
			//

			sse_vector result;
			
			if constexpr (MMAP)
				result = core_file_map<TH>(stats, file, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY,sq);
			else
				result = core_file_stream<TH>(stats, file, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY,sq);

			//Coalesce ids into single file handle and write it to store:
			//Identify as unique:
			//

			std::tie(key, id) = identify<TH>(domain, result);

			if (store.Is(id)) //TODO METADATA is not reported on this function
				return key;

			//Write unique block:
			//

			encode2<TH>(result, key, id,compression);

			store.Write(id, result); //Files larger than 32 GB exceed 1MB of metadata, breaking the unenforced limit.

			return key;
		}


		template < bool MMAP = true, typename TH, typename STORE, typename D > BlockResult single_file(std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1,size_t MAX_MEMORY=128*1024*1024)
		{
			BlockResult stats;

			if (GetFileSize(name) == 0)
				return stats;

			if constexpr (MMAP)
				stats.key_list = core_file_map<TH,STORE, D, BLOCK, THREADS>(stats, mio::mmap_source(name), store, domain, BLOCK, THREADS, compression, GROUP,MAX_MEMORY);
			else
				stats.key_list = core_file_stream<TH,STORE, D, BLOCK, THREADS>(stats, name, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY);

			return stats;
		}


		template < bool MMAP = true, typename TH, typename STORE, typename D> KeyResult<TH> submit_file(std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1,size_t MAX_MEMORY = 128*1024*1024)
		{
			Statistics stats;

			if (GetFileSize(name) == 0)
				return { TH(), stats.direct };

			TH key;

			if constexpr (MMAP)
				key = submit_core_map<TH>(stats, mio::mmap_source(name), store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY);
			else
				key = submit_core_stream<TH>(stats, name, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY);

			return { key, stats.direct };
		}

		template < bool MMAP = true, typename TH, typename STORE, typename D > sse_vector single_file2(Statistics& stats, std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t MAX_MEMORY=128*1024*1024,size_t sq = -1)
		{
			if (GetFileSize(name) == 0)
			{
				stats.atomic.sequence++;
				stats.atomic.files--;
				return sse_vector();
			}

			if constexpr (MMAP)
				return core_file_map<TH>(stats, mio::mmap_source(name), store, domain, BLOCK, THREADS, compression, GROUP,MAX_MEMORY,sq);
			else
				return core_file_stream<TH>(stats, name, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY,sq);
		}


		template < bool MMAP = true, typename TH, typename STORE, typename D> TH submit_file2(Statistics& stats, std::string_view name, STORE& store, const D& domain = default_domain, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1,size_t MAX_MEMORY=128*1024*1024,size_t sq=-1)
		{
			if (GetFileSize(name) == 0)
			{
				stats.atomic.sequence++;
				stats.atomic.files--;
				return TH();
			}

			if constexpr (MMAP)
				return submit_core<MMAP,TH>(stats, mio::mmap_source(name), store, domain, BLOCK, THREADS, compression, GROUP,MAX_MEMORY,sq);
			else
				return submit_core<MMAP,TH>(stats, name, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY,sq);
		}

		template < typename DITR, typename TH, typename ON_FILE > uint64_t core_delta(delta::Path<TH>& db, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel_count = 0)
		{
			uint64_t total_size = 0;

			for (auto& e : DITR(path, std::filesystem::directory_options::skip_permission_denied))
			{
				if (e.is_directory())
					continue;

				uint64_t change_time = GetFileWriteTime(e);

				std::string rel;
				try
				{
					rel = e.path().string();
				}
				catch (...)
				{
					//Todo store utf8
					//auto utf8_string = e.path().u8string(); 
					//rel = std::string(utf8_string.begin(), utf8_string.end());

					//Todo open on win32 with ucs16 u16string

					std::cout << "Skipping file with unicode characters in name... " << std::endl;
					continue;
				}
				
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

		template < bool MMAP = true, typename DITR, typename TH, typename STORE, typename ON_FILE, typename D > void core_folder(delta::Path<TH> & db, Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel_count = 0, size_t MAX_MEMORY = 128*1024*1024, bool use_sequence = false)
		{
			try
			{
				size_t sequence = 0;
				for (auto& e : DITR(path, std::filesystem::directory_options::skip_permission_denied))
				{
					if (e.is_directory())
						continue;

					stats.atomic.items++;

					uint64_t change_time = GetFileWriteTime(e);

					std::string full;

					try
					{
						full = e.path().string();
					}
					catch (...)
					{
						std::cout << "Skipping file with unicode characters in name..." << std::endl;
						continue;
					}

					auto rel = full.substr(path.size());

					uint64_t size = e.file_size();

					auto queue = db.Queue(rel, size, change_time, BLOCK, LARGE_THRESHOLD);

					if (!queue) //Excluded
						continue;

					if (!db.Changed(rel, size, change_time, queue))
					{
						stats.atomic.read += size;
						stats.atomic.blocks += (size / BLOCK + ((size % BLOCK) ? 1 : 0) /*+ ((size >= LARGE_THRESHOLD) ? 1 : 0) ... use this when the large file metadata is fixed*/);
						continue;
					}

					auto _file = [&](std::string handle, std::string name, uint64_t size, uint64_t changed, uint8_t* queue, size_t sq)
					{
						try
						{
							if (size >= LARGE_THRESHOLD)
							{
								auto key = submit_file2<MMAP,TH>(stats, handle, store, domain, BLOCK, THREADS, compression, GROUP,MAX_MEMORY,(use_sequence)?sq:-1);

								db.Apply(name, size, changed, key, queue);
							}
							else
							{
								auto block = single_file2<MMAP,TH>(stats, handle, store, domain, BLOCK, THREADS, compression, GROUP, MAX_MEMORY, (use_sequence) ? sq : -1);

								db.Apply(name, size, changed, block, queue);
							}
						}
						catch (const std::exception& ex)
						{
							std::cerr << "Error in File Thread: " << ex.what() << std::endl;
						}
						catch (...)
						{
							std::cout << "Unknown Problem with file thread." << std::endl;
						}
					};

					while (stats.atomic.files.load() >= FILES)
						std::this_thread::sleep_for(std::chrono::milliseconds(10));

					if (!on_file(rel, size, change_time))
						break;

					stats.atomic.files++;

					std::thread(_file, full, rel, size, change_time, queue, sequence++).detach();
				}
			}
			catch (...)
			{
				/*
					Allow all our threads to exit before we destroy their context.
				*/

				while (stats.atomic.files.load() || stats.atomic.threads.load())
					std::this_thread::sleep_for(std::chrono::milliseconds(100));

				throw;
			}
			
			while (stats.atomic.files.load() || stats.atomic.threads.load())
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}

		template < bool MMAP = true, typename DITR, typename TH, typename STORE, typename ON_FILE, typename D > TH submit_folder(std::string_view exclude, std::string_view delta_folder,Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			delta::Path<TH> db(delta_folder,exclude);

			db.OpenForWriting();

			core_folder<MMAP, DITR,TH>(db,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel,MAX_MEMORY,sequence);

			db.Statistics(stats,domain);

			stats.atomic.files++;

			return submit_file2<MMAP,TH>(stats, db.Finalize(), store, domain, BLOCK, THREADS, compression, GROUP);
		}

		template < typename DITR, typename TH, typename ON_FILE > uint64_t delta_folder(std::string_view exclude, std::string_view snapshot, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel = 0)
		{
			delta::Path<TH> db(snapshot,exclude);

			return core_delta<DITR,TH>( db, path, on_file, drive, rel);
		}

		template <typename TH,typename ON_FILE > uint64_t single_delta(std::string_view exclude, std::string_view snapshot, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel = 0)
		{
			return delta_folder< std::filesystem::directory_iterator,TH >(exclude, snapshot, path, on_file, drive, rel);
		}

		template < typename TH,typename ON_FILE> uint64_t recursive_delta(std::string_view exclude, std::string_view snapshot, std::string_view path, ON_FILE && on_file, std::string_view drive = "", size_t rel = 0)
		{
			return delta_folder< std::filesystem::recursive_directory_iterator,TH >(exclude, snapshot, path, on_file, drive, rel);
		}

		template < bool MMAP = true, typename TH, typename STORE, typename ON_FILE, typename D > KeyResult<TH> single_folder(std::string_view exclude, std::string_view delta_folder,std::string_view path, STORE& store, ON_FILE &&on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			Statistics stats;

			auto key = submit_folder< MMAP, std::filesystem::directory_iterator,TH>(exclude, delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel,MAX_MEMORY,sequence);

			return { key, stats.direct };
		}


		template < bool MMAP = true, typename TH,typename STORE, typename ON_FILE, typename D > KeyResult<TH> recursive_folder(std::string_view exclude, std::string_view delta_folder, std::string_view path, STORE& store, ON_FILE &&on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			Statistics stats;

			auto key = submit_folder<MMAP, std::filesystem::recursive_directory_iterator,TH>(exclude, delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel,MAX_MEMORY,sequence);

			return { key, stats.direct };
		}

		template < bool MMAP = true, typename TH,typename STORE, typename ON_FILE, typename D > TH single_folder2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive = "", size_t rel = 0, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			return submit_folder< MMAP, std::filesystem::directory_iterator,TH>(exclude,delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel,MAX_MEMORY,sequence);
		}

		template < bool MMAP = true, typename TH, typename STORE, typename ON_FILE, typename D > TH recursive_folder2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, std::string_view drive ="",size_t rel = 0, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			return submit_folder< MMAP, std::filesystem::recursive_directory_iterator,TH>(exclude, delta_folder,stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,drive,rel,MAX_MEMORY,sequence);
		}



#ifdef _WIN32

		template < bool MMAP = true, typename TH, typename STORE, typename ON_FILE, typename D > TH vss_folder2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view _path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence=false)
		{
			std::string full = std::filesystem::absolute(_path).string();

			bool use_root = false;// full.size() != _path.size();

			std::string path = full.substr(2);
			std::string drive = full.substr(0,1) + ":\\";

			volsnap::win32::Snapshot sn(drive);

			auto root = sn.Mount(std::filesystem::absolute(delta_folder).string() + "\\mount");

			std::string vss_path; vss_path += root; vss_path += path;

			size_t rel = std::count(vss_path.begin(), vss_path.end(), '\\') + std::count(vss_path.begin(), vss_path.end(), '/');

			auto r = recursive_folder2<MMAP,TH>(exclude, delta_folder,stats, vss_path,store,on_file,domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,(use_root) ? drive : "",rel,MAX_MEMORY,sequence);

			sn.Dismount();

			return r;
		}

		template < bool MMAP = true, typename TH,typename STORE, typename ON_FILE, typename D > KeyResult<TH> vss_folder(std::string_view exclude, std::string_view delta_folder, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			Statistics stats;

			auto r = vss_folder2<MMAP,TH>(exclude, delta_folder, stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,MAX_MEMORY,sequence);

			return { r, stats.direct };
		}

		template < bool MMAP = true, typename TH, typename STORE, typename ON_FILE, typename D > TH vss_single2(std::string_view exclude, std::string_view delta_folder, Statistics& stats, std::string_view _path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			std::string full = std::filesystem::absolute(_path).string();

			bool use_root = false;// full.size() != _path.size();

			std::string path = full.substr(2);
			std::string drive = full.substr(0, 1) + ":\\";

			volsnap::win32::Snapshot sn(drive);

			auto root = sn.Mount(std::filesystem::absolute(delta_folder).string() + "\\mount");

			std::string vss_path; vss_path += root; vss_path += path;

			size_t rel = std::count(vss_path.begin(), vss_path.end(), '\\') + std::count(vss_path.begin(), vss_path.end(), '/');

			auto r = single_folder2<MMAP,TH>(exclude, delta_folder, stats, vss_path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD, (use_root) ? drive : "", rel,MAX_MEMORY,sequence);

			sn.Dismount();

			return r;
		}

		template < bool MMAP = true, typename TH, typename STORE, typename ON_FILE, typename D > KeyResult<TH> vss_single(std::string_view exclude, std::string_view delta_folder, std::string_view path, STORE& store, ON_FILE && on_file, const D& domain = default_domain, size_t FILES = 1, size_t BLOCK = 1024 * 1024, size_t THREADS = 1, int compression = 5, size_t GROUP = 1, size_t LARGE_THRESHOLD = 128 * 1024 * 1024, size_t MAX_MEMORY = 128 * 1024 * 1024, bool sequence = false)
		{
			Statistics stats;

			auto r = vss_single2<MMAP,TH>(exclude, delta_folder, stats, path, store, on_file, domain, FILES, BLOCK, THREADS, compression, GROUP, LARGE_THRESHOLD,MAX_MEMORY,sequence);

			return { r, stats.direct };
		}

#endif
	}
}