/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <iostream>
#include <chrono>
#include <iomanip>

#include "d8u/memory.hpp"
#include "d8u/util.hpp"
#include "d8u/hash.hpp"
#include "d8u/random.hpp"
#include "d8u/crypto.hpp"
#include "d8u/alt_crypto.hpp"
#include "d8u/transform.hpp"

#include "hash/state.hpp"

namespace diagnose
{
	void self_diagnose()
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
}
