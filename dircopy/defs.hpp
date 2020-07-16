/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "d8u/transform.hpp"
#include "d8u/util.hpp"
#include "d8u/memory.hpp"

namespace dircopy
{
	namespace defs
	{
		using namespace d8u::transform;
		using namespace d8u::util;

#pragma pack( push, 1 )

		template < typename T = _DefaultHash > struct DiskHeader
		{
			uint64_t size;
			T block_zero;
			T block_n;
			Audit<T> audit;
		};

		template < typename T = _DefaultHash> struct VolumeHeader
		{
			T key;
			Audit<T> audit;
			uint32_t disk;
			uint32_t part;
			uint64_t start;
			uint64_t length;
			uint64_t used;
			uint64_t free;

			uint32_t cluster;
			uint8_t disk_type;
			uint8_t fs;
			uint16_t desc_len;
		};

		template < typename T = _DefaultHash> struct KeyResult
		{
			T key;
			Direct stats;
		};

		struct BlockResult
		{
			d8u::sse_vector key_list;
			Direct stats;
		};

#pragma pack(pop)
	}
}