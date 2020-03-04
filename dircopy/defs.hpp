/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "d8u/transform.hpp"

namespace dircopy
{
	namespace defs
	{
		using namespace d8u::transform;

#pragma pack( push, 1 )

		struct DiskHeader
		{
			uint64_t size;
			DefaultHash block_zero;
			DefaultHash block_n;
		};

		struct VolumeHeader
		{
			DefaultHash file;
			DefaultHash meta;
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

#pragma pack(pop)
	}
}