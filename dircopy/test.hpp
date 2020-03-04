/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <vector>
#include <string_view>

#include "backup.hpp"
#include "restore.hpp"
#include "validate.hpp"

#include "volstore/simple.hpp"
#include "d8u/util.hpp"
#include "d8u/compare.hpp"


using namespace dircopy;
using namespace d8u;


TEST_CASE("Simple File", "[volcopy::backup/restore]")
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


TEST_CASE("Complex File", "[volcopy::backup/restore]")
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


TEST_CASE("No Compress File", "[volcopy::backup/restore]")
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


TEST_CASE("Folder", "[volcopy::backup/restore]")
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

	auto result1 = backup::recursive_folder("delta","testdata", store,
		[](auto&, auto, auto) {}, util::default_domain, 5, 1024 * 1024, 8, 5, 8, 64 * 1024 * 1024);
	auto result2 = backup::recursive_folder("delta", "testdata", store,
		[](auto&, auto, auto) {}, util::default_domain, 1, 1024 * 1024, 1, 5, 1, 64 * 1024 * 1024);
	auto result3 = backup::vss_folder("altdelta", "testdata", store,
		[](auto&, auto, auto) {}, util::default_domain, 1, 1024 * 1024, 1, 5, 1, 64 * 1024 * 1024);


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


