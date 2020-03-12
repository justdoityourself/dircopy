/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <vector>
#include <string_view>

#include "backup.hpp"
#include "restore.hpp"
#include "validate.hpp"
#include "mount.hpp"

#include "volstore/simple.hpp"
#include "volstore/image.hpp"
#include "volstore/api.hpp"

#include "d8u/util.hpp"
#include "d8u/compare.hpp"


using namespace dircopy;
using namespace d8u;


#include "volrng/volume.hpp"
#include "volrng/compat.hpp"


TEST_CASE("Comprehensive folder structure (Net)", "[dircopy::backup/restore]")
{
	constexpr auto itr_count = 1;
	constexpr auto folder_size = util::_mb(100);

	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("teststore");

	std::filesystem::create_directories("tempdisk");
	std::filesystem::create_directories("testsnap");
	std::filesystem::create_directories("teststore");

	{
		volrng::volume::Test<volrng::DISK> handle("tempdisk");
		volstore::api::StorageService service("teststore", 1);

		std::this_thread::sleep_for(std::chrono::milliseconds(1000));

		volstore::BinaryStoreClient store;

		for (size_t i = 0; i < itr_count; i++)
		{
			handle.Run(folder_size, volrng::MOUNT);

			handle.Mount(volrng::MOUNT);

			auto result = backup::vss_folder("", "testsnap", volrng::MOUNT, store,
				[](auto&, auto, auto) { return true; },
				util::default_domain, 64, util::_mb(1), 64, 5, 64, util::_mb(64));

			handle.Dismount();

			{
				volrng::DISK res_disk("resdisk.img", util::_gb(100), volrng::MOUNT2);

				restore::folder(volrng::MOUNT2, result.key, store, util::default_domain, true, true, util::_mb(1), util::_mb(64), 64, 64);
				CHECK(handle.Validate(volrng::MOUNT2));
			}

			std::filesystem::remove_all("resdisk.img");
		}
	}

	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("teststore");
}

TEST_CASE("File Exclusion", "[dircopy::backup]")
{
	std::filesystem::remove_all("delta");
	std::filesystem::remove_all("teststore");
	std::filesystem::create_directories("teststore");

	std::string_view exclude = R"(
    {
        "file": 
		{ 
			"testdata\large_compress":true,
			"testdata\empty":true,
			"testdata\small_compress":true,
			"testdata\tiny_compress":true,
			"testdata\tiny_nocompress":true,
			"testdata\medium_nocompress":true,
		},
		"path":{}
    })";

	volstore::Simple store("teststore");

	auto result = backup::recursive_folder(exclude, "delta", "testdata", store,
		[](auto&, auto, auto) { return true; }, util::default_domain, 5, 1024 * 1024, 8, 5, 8, 64 * 1024 * 1024);

	CHECK(1024 * 1024 /*Database Block*/ == result.stats.read);

	std::filesystem::remove_all("teststore");
	std::filesystem::remove_all("delta");
}

TEST_CASE("Path Exclusion", "[dircopy::backup]")
{
	std::filesystem::remove_all("delta");
	std::filesystem::remove_all("teststore");
	std::filesystem::create_directories("teststore");

	std::string_view exclude = R"(
    {
        "file": { },
		"path":
		{
			"testdata":true
		}
    })";

	volstore::Simple store("teststore");

	auto result = backup::recursive_folder(exclude, "delta", "testdata", store,
		[](auto&, auto, auto) { return true; }, util::default_domain, 5, 1024 * 1024, 8, 5, 8, 64 * 1024 * 1024);

	CHECK(1024*1024 /*Database Block*/ == result.stats.read);

	std::filesystem::remove_all("teststore");
	std::filesystem::remove_all("delta");
}

TEST_CASE("Comprehensive folder structure (Image)", "[dircopy::backup/restore]")
{
	constexpr auto itr_count = 3;
	constexpr auto folder_size = util::_mb(100);

	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("teststore");

	std::filesystem::create_directories("tempdisk");
	std::filesystem::create_directories("testsnap");
	std::filesystem::create_directories("teststore");

	{
		volrng::volume::Test<volrng::DISK> handle("tempdisk");
		volstore::Image store("teststore");

		for (size_t i = 0; i < itr_count; i++)
		{
			handle.Run(folder_size, volrng::MOUNT);

			handle.Mount(volrng::MOUNT);

			auto result = backup::vss_folder("","testsnap", volrng::MOUNT, store,
				[](auto&, auto, auto) { return true; },
				util::default_domain, 8, util::_mb(1), 8, 5, 1, util::_mb(64));

			handle.Dismount();

			{
				volrng::DISK res_disk("resdisk.img", util::_gb(100), volrng::MOUNT2);

				restore::folder(volrng::MOUNT2, result.key, store, util::default_domain, true, true, util::_mb(1), util::_mb(64), 8, 8);
				CHECK(handle.Validate(volrng::MOUNT2));
			}

			std::filesystem::remove_all("resdisk.img");
		}
	}

	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("teststore");
}


TEST_CASE("Mount", "[dircopy::backup/restore]")
{
	std::filesystem::remove_all("mount");
	std::filesystem::remove_all("delta");
	std::filesystem::remove_all("teststore");
	std::filesystem::create_directories("teststore");
	std::filesystem::create_directories("mount");

	volstore::Simple store("teststore");

	auto result = backup::recursive_folder("", "delta", "testdata", store, [](auto&, auto, auto) { return true; }, util::default_domain, 5, 1024 * 1024, 8, 5, 8, 64 * 1024 * 1024);

	mount::Path handle(result.key, store, util::default_domain, true);

	CHECK(1 == handle.Search("ge_com", [](auto s, auto t, auto n, auto k) {return true; }));
	CHECK(1 == handle.Search("medium", [](auto s, auto t, auto n, auto k) {return true; }));
	CHECK(2 == handle.Search("tiny", [](auto s, auto t, auto n, auto k) {return true; }));
	CHECK(5 == handle.Search("compress", [](auto s, auto t, auto n, auto k) {return true; }));
	CHECK(2 == handle.Search("nocompress", [](auto s, auto t, auto n, auto k) {return true; }));

	size_t count = 0;

	handle.Enumerate([&](auto s, auto t, auto n, auto k)
	{
		count++;
		return true;
	});

	CHECK(6 == count);

	std::filesystem::remove_all("mount");
	std::filesystem::remove_all("delta");
	std::filesystem::remove_all("teststore");
}


TEST_CASE("Simple File", "[dircopy::backup/restore]")
{
	std::filesystem::remove_all("restore");
	std::filesystem::remove_all("teststore");

	std::filesystem::create_directories("teststore");
	std::filesystem::create_directories("restore");

	volstore::Simple store("teststore");

	auto result1 = backup::submit_file("testdata/small_compress", store, util::default_domain);
	auto result2 = backup::submit_file("testdata/small_compress", store, util::default_domain);

	CHECK(std::equal(result1.key.begin(), result1.key.end(), result2.key.begin()));
	CHECK(result2.stats.duplicate == result1.stats.read);

	CHECK(validate::file(result2.key, store, util::default_domain,4).first);
	CHECK(validate::deep_file(result2.key, store, util::default_domain,4).first);

	restore::file("restore/file1", result1.key, store, util::default_domain, true, true, 4);
	CHECK(compare::files_bytes("testdata/small_compress", "restore/file1"));

	restore::file("restore/file2", result2.key, store, util::default_domain, true, true);
	CHECK(compare::files("testdata/small_compress", "restore/file2"));

	std::filesystem::remove_all("teststore");
	std::filesystem::remove_all("restore");
}


TEST_CASE("Complex File", "[dircopy::backup/restore]")
{
	std::filesystem::remove_all("restore");
	std::filesystem::remove_all("teststore");

	std::filesystem::create_directories("teststore");
	std::filesystem::create_directories("restore");

	volstore::Simple store("teststore");

	auto control = backup::submit_file("testdata/small_compress", store, util::default_domain, 1024 * 1024, 2, 5, 6);
	auto result2 = backup::submit_file("testdata/small_compress", store, util::default_domain);

	CHECK(std::equal(control.key.begin(), control.key.end(), result2.key.begin()));
	CHECK(result2.stats.duplicate == control.stats.read);

	CHECK(validate::file(result2.key, store, util::default_domain,4).first);
	CHECK(validate::deep_file(result2.key, store, util::default_domain,4).first);

	restore::file("restore/control", control.key, store, util::default_domain, true, true);
	CHECK(compare::files_bytes("testdata/small_compress", "restore/control"));

	restore::file("restore/file2", result2.key, store, util::default_domain, true, true, 4);
	CHECK(compare::files("testdata/small_compress", "restore/file2"));

	std::filesystem::remove_all("teststore");
	std::filesystem::remove_all("restore");
}


TEST_CASE("No Compress File", "[dircopy::backup/restore]")
{
	std::filesystem::remove_all("restore");
	std::filesystem::remove_all("teststore");

	std::filesystem::create_directories("teststore");
	std::filesystem::create_directories("restore");

	volstore::Simple store("teststore");

	auto control = backup::submit_file("testdata/tiny_nocompress", store, util::default_domain, 1024 * 1024, 2, 5, 6);
	auto result2 = backup::submit_file("testdata/tiny_nocompress", store, util::default_domain);

	CHECK(std::equal(control.key.begin(), control.key.end(), result2.key.begin()));
	CHECK(result2.stats.duplicate == control.stats.read);

	CHECK(validate::file(result2.key, store, util::default_domain,4).first);
	CHECK(validate::deep_file(result2.key, store, util::default_domain,4).first);

	restore::file("restore/control", control.key, store, util::default_domain, true, true);
	CHECK(compare::files_bytes("testdata/tiny_nocompress", "restore/control"));

	restore::file("restore/file2", result2.key, store, util::default_domain, true, true);
	CHECK(compare::files("testdata/tiny_nocompress", "restore/file2"));

	std::filesystem::remove_all("teststore");
	std::filesystem::remove_all("restore");
}


TEST_CASE("Folder", "[dircopy::backup/restore]")
{
	std::filesystem::remove_all("restore1");
	std::filesystem::remove_all("delta");
	std::filesystem::remove_all("altdelta");
	std::filesystem::remove_all("restore2");
	std::filesystem::remove_all("restore3");
	std::filesystem::remove_all("teststore");
	std::filesystem::create_directories("teststore");
	std::filesystem::create_directories("restore1");
	std::filesystem::create_directories("restore2");
	std::filesystem::create_directories("restore3");

	volstore::Simple store("teststore");

	auto result1 = backup::recursive_folder("", "delta","testdata", store,
		[](auto&, auto, auto) { return true; }, util::default_domain, 5, 1024 * 1024, 8, 5, 8, 64 * 1024 * 1024);
	auto result2 = backup::recursive_folder("", "delta", "testdata", store,
		[](auto&, auto, auto) { return true; }, util::default_domain, 3, 1024 * 1024, 6, 5, 1, 64 * 1024 * 1024);
	auto result3 = backup::vss_folder("", "altdelta", "testdata", store,
		[](auto&, auto, auto) { return true; }, util::default_domain, 16, 1024 * 1024, 16, 5, 16, 64 * 1024 * 1024);


	CHECK(std::equal(result1.key.begin(), result1.key.end(), result2.key.begin()));
	CHECK(result3.stats.duplicate == result1.stats.read);

	CHECK(validate::folder(result2.key, store, util::default_domain, 1024 * 1024, 64 * 1024 * 1024,4,4).first);
	CHECK(validate::deep_folder(result2.key, store, util::default_domain, 1024 * 1024, 64 * 1024 * 1024,4,4).first);

	restore::folder("restore1", result1.key, store, util::default_domain, true, true, 1024 * 1024, 64 * 1024 * 1024, 8,8);
	CHECK(compare::folders("testdata", "restore1",8));

	restore::folder("restore2", result2.key, store, util::default_domain, true, true, 1024 * 1024, 64 * 1024 * 1024, 8,8);
	CHECK(compare::folders("testdata", "restore2",8));

	restore::folder("restore3", result3.key, store, util::default_domain, true, true, 1024 * 1024, 64 * 1024 * 1024, 8,8);
	CHECK(compare::folders("testdata", "restore3",8));

	std::filesystem::remove_all("teststore");
	std::filesystem::remove_all("restore1");
	std::filesystem::remove_all("restore2");
	std::filesystem::remove_all("restore3");
	std::filesystem::remove_all("delta");
	std::filesystem::remove_all("altdelta");
}
