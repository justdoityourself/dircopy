/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <iostream>
#include <chrono>
#include <iomanip>
#include <string_view>
#include <sstream>

#include "d8u/memory.hpp"
#include "d8u/util.hpp"
#include "d8u/hash.hpp"
#include "d8u/random.hpp"
#include "d8u/crypto.hpp"
#include "d8u/alt_crypto.hpp"
#include "d8u/transform.hpp"
#include "d8u/json.hpp"

#include "hash/state.hpp"

#include "volrng/platform.hpp"
#include "volrng/volume.hpp"

namespace diagnose
{
	void benchmark()
	{
		using namespace std::chrono;
		using namespace d8u;
		using namespace d8u::util;
		using fast_hash = d8u::custom_hash::DefaultHashT<template_hash::stateful::State_16_32_1>;

		constexpr size_t rep = 100;

		auto & o = std::cout;
		auto e = "\r\n";
		auto de = "\r\n\r\n";
		auto qe = "\r\n\r\n\r\n\r\n";

		o << fixed << std::setprecision(2);

		aligned_vector random_buffer(_mb(1));

		for (auto& e : random_buffer)
			e = random::Integer();

		auto time = [&](auto&& f)
		{
			auto t1 = high_resolution_clock::now();

			f();

			auto t2 = high_resolution_clock::now();

			return (t2.time_since_epoch() - t1.time_since_epoch()).count();
		};

		auto repeat = [&](size_t count, auto && f)
		{
			time_t sum = 0;

			for (size_t i = 0; i < count; i++)
				sum += time(f);

			return (double)sum;
		};

		auto mbs = [&](auto && f)
		{
			double sec = repeat(rep, f) / 1000 / 1000 / 1000;

			return (double)rep / sec;
		};



		o << "Benchmarks: " << qe;


		o << "Hash: " << de;

		o << "SHA256: " << mbs([&]()
		{
			transform::_DefaultHash sha256(random_buffer);
		}) << "MB/s" << e;

		o << "xFH16_4_1: " << mbs([&]()
		{
			fast_hash fh(random_buffer);
		}) << "MB/s" << e;

		o << "SHA512: " << mbs([&]()
		{
			transform::Password sha512(random_buffer);
		}) << "MB/s" << e;


		o << de << "Encrypt: " << de;

		transform::Password pw(random_buffer);

		o << "AES256: " << mbs([&]()
		{
			transform::encrypt(random_buffer,pw);
		}) << "MB/s" << e;

		o << "PCF256: " << mbs([&]()
		{
			alt_crypto::pcf256_enc(random_buffer, pw);
		}) << "MB/s" << e;


		o << de << "Decrypt: " << de;

		o << "AES256: " << mbs([&]()
		{
			transform::decrypt(random_buffer,pw);
		}) << "MB/s" << e;

		o << "PCF256: " << mbs([&]()
		{
			alt_crypto::pcf256_dec(random_buffer, pw);
		}) << "MB/s" << e;


		o << de << "Compression: " << de;

		o << "LZO LVL1: " << mbs([&]()
		{
			transform::minilzo_compress2(random_buffer, 1);
		}) << "MB/s" << e;

		o << "LZO LVL5: " << mbs([&]()
		{
			transform::minilzo_compress2(random_buffer, 5);
		}) << "MB/s" << e;

		o << "LZO LVL9: " << mbs([&]()
		{
			transform::minilzo_compress2(random_buffer, 9);
		}) << "MB/s" << e;

		o << "GZIP LVL1: " << mbs([&]()
		{
			transform::gzip_compress2(random_buffer, 1);
		}) << "MB/s" << e;

		o << "GZIP LVL5: " << mbs([&]()
		{
			transform::gzip_compress2(random_buffer, 5);
		}) << "MB/s" << e;

		o << "GZIP LVL9: " << mbs([&]()
		{
			transform::gzip_compress2(random_buffer, 9);
		}) << "MB/s" << e;

		o << "LZMA LVL1: " << mbs([&]()
		{
			transform::lzma_compress2(random_buffer, 1);
		}) << "MB/s" << e;

		o << "LZMA LVL5: " << mbs([&]()
		{
			transform::lzma_compress2(random_buffer, 5);
		}) << "MB/s" << e;

		o << "LZMA LVL9: " << mbs([&]()
		{
			transform::lzma_compress2(random_buffer, 9);
		}) << "MB/s" << e;
	}

	void workflow(std::string_view bin)
	{
		constexpr size_t rounds = 100;

		constexpr std::string_view green("\033[32m"), yellow("\033[33m"), white("\033[0m"), path("endless_chain"), snap("snapshot"), image("image"), password("password");



		auto run = [&](auto const& ... args)
		{
			std::ostringstream _cmd;

			_cmd << bin << " ";

			((_cmd << args << " "), ...);
			
			auto cmd = _cmd.str();

			std::cout << green << "Run: " << cmd << yellow << std::endl << std::endl;

			system(cmd.c_str());

			std::cout << std::endl << std::endl << white;
		};

		auto reset = [&]()
		{
			volrng::DISK::Dismount("endless_chain\\disk.img");
			std::filesystem::remove_all(path);
			std::filesystem::remove_all("restore");
			std::filesystem::remove_all(snap);
			std::filesystem::remove_all(image);
			std::filesystem::remove_all("result.txt");
		};



		reset();

		std::thread server([&]()
		{
			run("-z --silent -ah --path", image);
		});

		std::this_thread::sleep_for(std::chrono::milliseconds(3 * 1000));


		for (size_t i = 0; i < rounds; i++)
		{
			std::cout << white << "Round " << i << " / " << rounds << std::endl << std::endl;

			run("-a rng_step --snapshot", path, "--path Z:");

			run("-a rng_mount --snapshot", path, "--path Z:");

			run("-a backup -ah --compression 21 --description ImageDescriptionText -h 127.0.0.1 --snapshot", snap, "--path Z:/", "--security",password);

			run("-a latest --silent -h 127.0.0.1 --security > result.txt", password, "--snapshot", snap);

			d8u::json::JsonMapS map(std::string_view("result.txt"));

			std::string_view v = map("data")["key"];

			run("-a rng_dismount --snapshot", path, "--path Z:");

			run("-a restore -ah -h 127.0.0.1 -k", v, "--path restore");
		}


		std::cout << "Finished, shuting down server..." << std::endl;
		std::this_thread::sleep_for(std::chrono::milliseconds(11*1000));

		server.join(); //Todo send shutdown signal

		reset();
	}

	void self_diagnose(std::string_view bin)
	{
		//benchmark();
		workflow(bin);
	}
}
