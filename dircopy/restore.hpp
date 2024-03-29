/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>
#include <fstream>

#include "d8u/transform.hpp"
#include "defs.hpp"
#include "delta.hpp"
#include "d8u/memory.hpp"

#include "d8u/util.hpp"
#include "../mio.hpp"

namespace dircopy
{
	namespace restore
	{
		using namespace defs;
		using namespace d8u::util;
		using namespace d8u::transform;



		template <typename TH,typename S, typename D> d8u::sse_vector block(Statistics & s,const TH& key, S& store, const D& domain, bool validate = false)
		{
			auto file_id = key.GetNext();
			auto block = store.Read(file_id);

			s.atomic.blocks++;
			s.atomic.read += block.size();

			decode(domain, block, key);

			if (validate)
			{
				TH dup_key(domain, block);

				if (!std::equal(key.begin(), key.end(), dup_key.begin()))
					throw std::runtime_error("Corrupt Block");
			}

			return block;
		}

		template <typename TH, typename S, typename D> d8u::sse_vector file_memory(Statistics& s, span<TH> keys, S& store, const D& domain, bool validate_blocks = false, bool hash_file = false)
		{
			d8u::sse_vector result;
			result.reserve(keys.size() * 1024 * 1024);

			typename TH::State state;

			if (hash_file)
				state.Update(domain);

			for (auto& key : keys)
			{
				if (&key == keys.end() - 1)
					break; //Last hash is the file hash

				auto buffer = block(s,key, store, domain, validate_blocks);

				if (hash_file)
					state.Update(buffer);

				result.insert(result.end(), buffer.data(), buffer.data() + buffer.size());
			}

			if (hash_file)
			{
				auto final_hash = state.Finish();

				if (!std::equal(final_hash.begin(), final_hash.end(), (uint8_t*)(keys.end() - 1)))
					throw std::runtime_error("Corrupt File");
			}

			return result;
		}

		template <typename TH, typename S, typename D> void _file2(Statistics& s, std::string_view dest, span<TH> keys, S& store, const D& domain, bool validate_blocks = false, bool hash_file = false, size_t P = 1)
		{
			typename TH::State state;

			if (hash_file)
				state.Update(domain);

			std::filesystem::create_directories(std::filesystem::path(dest).parent_path().string());

			std::ofstream output(dest, std::ios::binary);

			if (!output.is_open())
				throw std::runtime_error("Failed to create file");

			if (P == 1)
			{
				for (auto& key : keys)
				{
					if (&key == keys.end() - 1)
						break; //Last hash is the file hash

					auto buffer = block(s,key, store, domain, validate_blocks);

					if (hash_file)
						state.Update(buffer);

					s.atomic.write += buffer.size();

					output.write((char*)buffer.data(), buffer.size());
				}
			}
			else
			{
				std::atomic<int> local = 0;
				std::vector<d8u::sse_vector> map; map.resize(keys.size() - 1);

				std::thread io([&]()
				{
					for (auto& e : map)
					{
						while (!e.size())
							std::this_thread::sleep_for(std::chrono::milliseconds(10));

						if (hash_file)
							state.Update(e);

						s.atomic.write += e.size();

						output.write((char*)e.data(), e.size());

						e.clear();
					}
				});

				for (size_t i = 0; i < keys.size() - 1; i++)
				{
					while (local.load() >= P)
						std::this_thread::sleep_for(std::chrono::milliseconds(10));

					local++;

					std::thread([&](size_t dx)
					{
						map[dx] = block(s,keys[dx], store, domain, validate_blocks);
						local--;
					}, i).detach();
				}

				io.join();
			}

			if (hash_file)
			{
				auto final_hash = state.Finish();

				if (!std::equal(final_hash.begin(), final_hash.end(), (uint8_t*)(keys.end() - 1)))
					throw std::runtime_error("Corrupt File");
			}
		}

		template <typename TH, typename S, typename D> void file2(Statistics& s, std::string_view dest, const TH& file_key, S& store, const D& domain, bool validate_blocks = false, bool hash_file = false, size_t P = 1)
		{
			auto file_record = block(s,file_key, store, domain, validate_blocks);

			if (file_record.size() % sizeof(TH) != 0)
				throw std::runtime_error("Malformed File Record");

			auto keys = span<TH>((TH*)file_record.data(), file_record.size() / sizeof(TH));

			_file2(s,dest, keys, store, domain, validate_blocks, hash_file, P);
		}

		template <typename TH, typename S, typename D> void folder2(Statistics & s,std::string_view dest, const TH& folder_key, S& store, const D& domain, bool validate_blocks = false, bool hash_file = false, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			auto folder_record = block(s, folder_key, store, domain, validate_blocks);

			auto database = restore::file_memory(s, gsl::span<TH>((TH*)folder_record.data(), folder_record.size() / sizeof(TH)), store, domain, validate_blocks, hash_file);

			typename tdb::MemoryHashmap db(database);

			{
				auto [size, time, name, data] = delta::Path<TH>::DecodeRaw(db.FindObject(domain));

				auto stats = (typename delta::Path<TH>::FolderStatistics * )data.data();

				s.direct.target = stats->size;
			}

			auto file = [&](uint64_t p)
			{
				dec_scope lock(s.atomic.files);

				auto [size, time, name, keys] = delta::Path<TH>::Decode(db.GetObject(p));

				if (!size)
				{
					if (name.size() > 3 && name[0] == '|' && name[1] == '|' && name[2] == '|')
						return true; //Stats block

					d8u::util::empty_file(std::string(dest) + "\\" + string(name));
					return true;
				}

				if (keys.size() == 1)
				{
					//if (keys.size() != 1)
					//	throw std::runtime_error("Malformed Folder Record ( 1 )");

					file2(s, std::string(dest) + "\\" + string(name), *keys.data(), store, domain, validate_blocks, hash_file, P);
				}
				else
				{
					if (keys.size() <= 1)
						throw std::runtime_error("Malformed Folder Record ( 2 )");

					_file2(s, std::string(dest) + "\\" + string(name), keys, store, domain, validate_blocks, hash_file, P);
				}

				return true;
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
					fast_wait(s.atomic.files,F);

					s.atomic.files++;

					std::thread(file, p).detach();

					return true;
				});

				fast_wait(s.atomic.files);
			}
		}

		template <typename TH, typename S, typename D> Direct file( std::string_view dest, const TH& file_key, S& store, const D& domain, bool validate_blocks = false, bool hash_file = false, size_t P = 1)
		{
			Statistics s;
		
			file2(s,dest,file_key,store,domain,validate_blocks,hash_file,P);

			return s.direct;
		}

		template <typename TH, typename S, typename D> Direct folder(std::string_view dest, const TH& folder_key, S& store, const D& domain, bool validate_blocks = false, bool hash_file = false, size_t BLOCK = 1024 * 1024, size_t THRESHOLD = 128 * 1024 * 1024, size_t P = 1, size_t F = 1)
		{
			Statistics s;

			folder2(s,dest, folder_key, store, domain, validate_blocks, hash_file, BLOCK, THRESHOLD, P, F);

			return s.direct;
		}
	}
}