/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#ifdef TEST_RUNNER


#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "dircopy/test.hpp"

int main(int argc, char* argv[])
{
    return Catch::Session().run(argc, argv);
}


#endif //TEST_RUNNER


#if ! defined(TEST_RUNNER)

#ifdef _WIN32

#include "volsnap/snapshot.hpp"

#endif

#include "volstore/image.hpp"
#include "volstore/binary.hpp"
#include "volstore/api.hpp"



#include "clipp.h"

#include <string>
#include <filesystem>
#include <iostream>
#include <thread>
#include <filesystem>

#include "d8u/string_switch.hpp"
#include "d8u/util.hpp"
#include "d8u/string.hpp"
#include "d8u/transform.hpp"
#include "d8u/json.hpp"
#include "d8u/compare.hpp"
#include "d8u/memory.hpp"

#include "dircopy/backup.hpp"
#include "dircopy/validate.hpp"
#include "dircopy/mount.hpp"
#include "dircopy/diagnose.hpp"

#include "blocksync/sync.hpp"

#include "hash/state.hpp"

#include "kreg/client.hpp"
#include "kreg/local.hpp"

#include "volrng/platform.hpp"
#include "volrng/volume.hpp"

using std::string;
using namespace clipp;
using d8u::switch_t;
using namespace dircopy;
using namespace volstore::api;

/*


        -i cli/image -a backup -p testdata -s cli/snap --compression 21
        -ac -ah -i cli/image -a backup -p testdata -s cli/snap --compression 21

        -ah -i cli/image -a validate -k cc7ae0ab563ecbb9a124b427757b5cd75d58a6a768ab15a2c7cdf640d54789e1

        -i cli/image -a validate_deep -k ea9d02542976326f612d79fe67676b1be5ecdb571564a89871977159af5d2de2




    CLI:

    -i cli/image -a backup -p testdata -s cli/snap

        Recursive Directory Backup: testdata; State: cli/snap; Domain: c522acad91dd42c690c8ee60820b1879

        Key: e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Threads 0, Files 0, Read 130940761, Write 92648156, Duplicate 0
        Finished in: 5.34995s

    -i cli/image -a validate_deep -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Validate Directory Deep:  Domain: c522acad91dd42c690c8ee60820b1879

        Validation Success
        Threads 0, Files 0, Read 92648156, Write 0, Duplicate 0
        Finished in: 7.0275s



    -i cli/image -a validate -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Validate Directory:  Domain: c522acad91dd42c690c8ee60820b1879

        Validation Success
        Threads 0, Files 0, Read 5045, Write 0, Duplicate 0
        Finished in: 1.0182s



    -i cli/image -a search -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511 -p compress

        Search: compress; Domain: c522acad91dd42c690c8ee60820b1879

        testdata\tiny_nocompress 141962 bytes 1583083122 changed
        testdata\tiny_compress 347136 bytes 1583083119 changed
        testdata\large_compress 93789184 bytes 1583083100 changed
        testdata\medium_nocompress 30510287 bytes 1583083110 changed
        testdata\small_compress 5103616 bytes 1583083116 changed
        Found 5 files

        Threads 0, Files 0, Read 5177, Write 0, Duplicate 0
        Finished in: 1.0238s



    -i cli/image -a restore -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511 -p cli/restore

        Restore: cli/restore; Domain: c522acad91dd42c690c8ee60820b1879

        Threads 0, Files 0, Read 92648288, Write 129892185, Duplicate 0
        Finished in: 2.34409s



    -i cli/image -a fetch -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511 -p testdata\tiny_compress -d cli/fetch

        Fetch: testdata\tiny_compress >> cli/fetch; Domain: c522acad91dd42c690c8ee60820b1879

        Threads 0, Files 0, Read 213013, Write 347136, Duplicate 0
        Finished in: 1.05452s



    -i cli/image -a enumerate -k e52b3b6bc693195ffd4a3bc88f0a2db931aacf82d4721556c64b9cb34dbfc511

        Enumerate:  Domain: c522acad91dd42c690c8ee60820b1879

        testdata\tiny_nocompress 141962 bytes 1583083122 changed
        testdata\tiny_compress 347136 bytes 1583083119 changed
        testdata\large_compress 93789184 bytes 1583083100 changed
        testdata\medium_nocompress 30510287 bytes 1583083110 changed
        testdata\small_compress 5103616 bytes 1583083116 changed
        Threads 0, Files 0, Read 5177, Write 0, Duplicate 0
        Finished in: 1.02567s



    -i cli/image -a delta -p testdata -s cli/snap

        testdata\New Text Document.txt 16 bytes 1583451225 changed

        Finished in: 1.02594s
*/

template < typename T > struct tag_t { using type = T; };

int main(int argc, char* argv[])
{
    bool vss = false, recursive = true, storage_server = false, scope = false;
    string path = "", snapshot = "", host = "", image = "", action = "backup", skey = "", dest = "", json = "", sdomain="", password="",description="";
    string hport = "8008", qport="9009", rport="1010", wport="1111";
    size_t threads = 4;
    size_t files = 64;
    size_t net_buffer = 16;
    size_t max_memory = 128;
    bool validate = false, auto_clear_bad_state = false, disable_mapping = true, aux_hash = false, sequence = false,index=false, help = false;

    size_t compression = 13;
    size_t block_grouping = 16;
    d8u::sse_vector domain;

    auto cli = (
        option("-c", "--config").doc("Json configuration file") & value("json", json),
        option("-k", "--key").doc("The store key used to restore, mount or validate") & value("key", skey),
        option("-a", "--action").doc("What action will be taken, backup, validate_deep, validate, delta, search, restore, fetch, enumerate, compare, sync, migrate, list, diagnose") & value("action", action),
        option("-s", "--snapshot").doc("A path where snapshot databases are stored") & value("snapshot", snapshot),
        option("-i", "--image").doc("Path of the image: D:\\Backup") & value("image", image),
        option("-h", "--host").doc("Hostname or IP of  store: backup.com, 192.168.4.14") & value("host", host),
        option("-p", "--path").doc("Path to backup") & value("directory", path),
        option("-d", "--destination").doc("Path to backup") & value("destination", dest),
        option("-o", "--domain").doc("Block identification salt") & value("domain", sdomain),
        option("-se", "--security").doc("Encrypt Metadata with this password") & value("password", password),
        option("-de", "--description").doc("String encrypted and stored with metadata") & value("description", description),
        option("-t", "--threads").doc("Threads used to encode / decode") & value("threads", threads),
        option("-nb", "--netbuffer").doc("Size of the TCP socket buffer") & value("network buffer", net_buffer),
        option("-mm", "--maxmemory").doc("Limit memory that can be used as IO buffer") & value("max memory", max_memory),
        option("-b", "--blockgroup").doc("Group size of identification query") & value("block_grouping", block_grouping),
        option("-m", "--compression").doc("Compression Level ( 0 - 19 )") & value("compression", compression),
        option("-f", "--files").doc("Files processed at a time") & value("threads", files),
        option("-v", "--vss").doc("Use vss snapshot").set(vss),
        option("-sc", "--scope").doc("Calculate Folder Size to enable progress").set(scope),
        option("-z", "--server").doc("Host block storage server").set(storage_server),
        option("-x", "--validate").doc("Validate Blocks that are read or restored").set(validate),
        option("-dx", "--index").doc("Index data for fast search").set(index),
        option("-hp", "--help").doc("Print CLI Documentaion").set(help),
        option("-sq", "--sequence").doc("Validate Blocks that are read or restored").set(sequence),
        option("-dm", "--disable_mapping").doc("used buffered io instead of memory mapping").set(disable_mapping),
        option("-ah", "--aux_hash").doc("Use faster hash for slower hardware").set(aux_hash),
        option("-ac", "--auto_clear_bad_state").doc("Recover with any errors from previous backup failures.").set(auto_clear_bad_state),
        option("-r", "--recursive").doc("Recursive enumeration of directories").set(recursive),
        option("-ph", "--httpport").doc("HTTP Port") & value("hport", hport),
        option("-pq", "--queryport").doc("Query Port") & value("qport", qport),
        option("-pr", "--readport").doc("Read Port") & value("rport", rport),
        option("-pw", "--writeport").doc("Write Port") & value("wport", wport)
        );

    bool running = true;
    d8u::util::Statistics _stats;
    d8u::util::Statistics* pstats = &_stats;

    using fast_hash = d8u::custom_hash::DefaultHashT<template_hash::stateful::State_16_32_1>;
    //using fast_hash = d8u::custom_hash::DefaultHashT<template_hash::stateful::State_8_32_1>;

    StorageService2<d8u::transform::_DefaultHash>* pservice2 = nullptr;
    StorageService2<fast_hash>* pservice1 = nullptr;

    volstore::BinaryStoreClient2<>* pclient = nullptr;
    std::string current_file,pk;

    auto start = std::chrono::high_resolution_clock::now();

    std::thread console([&]()
    {
        while (running)
        {
            pstats->Print();

            if(pservice1)
                std::cout << "[ C: " << pservice1->ConnectionCount() 
                << " M: " << pservice1->MessageCount() 
                << " S: " << pservice1->EventsStarted() 
                << " F: " << pservice1->EventsFinished() 
                << " R: " << pservice1->ReplyCount()  << " ]\r" << std::flush;
            else if (pservice2)
                std::cout << "[ C: " << pservice2->ConnectionCount()
                << " M: " << pservice2->MessageCount()
                << " S: " << pservice2->EventsStarted()
                << " F: " << pservice2->EventsFinished()
                << " R: " << pservice2->ReplyCount() << " ]\r" << std::flush;
            else if(pclient)
                std::cout << "[ W: " << pclient->Writes() << " R: " << pclient->Reads() << " ] " << current_file << "\t\t\r" << std::flush;
            else
                std::cout << current_file << "\t\t\r" << std::flush;

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    });

    try
    {
        if (!parse(argc, argv, cli))
        {
            std::cout << make_man_page(cli, argv[0]);
            throw std::runtime_error("Bad CLI");
        }

        if (json.size() && std::filesystem::exists(json))
        {
            d8u::json::JsonMap map(json);

            map.ForEachValue([&](auto key, auto value)
            {
                switch(switch_t(key))
                {
                case switch_t("vss"):   vss = value;    break;
                case switch_t("key"):   skey = value;   break;
                case switch_t("host"):  host = value;   break;
                case switch_t("path"):  path = value;   break;
                case switch_t("files"):     files = value;      break;
                case switch_t("scope"):     scope = value;      break;
                case switch_t("image"):     image = value;      break;
                case switch_t("action"):    action = value;     break;
                case switch_t("domain"):    sdomain = value;    break;
                case switch_t("threads"):   threads = value;    break;
                case switch_t("snapshot"):      snapshot = value;       break;
                case switch_t("validate"):      validate = value;       break;
                case switch_t("netbuffer"):     net_buffer = value;     break;
                case switch_t("recursive"):     recursive = value;      break;
                case switch_t("blockgroup"):    block_grouping = value; break;
                case switch_t("destination"):   dest = value;           break;
                case switch_t("compression"):   compression = value;    break;
                }
            });
        }

        if (help)
            std::cout << make_man_page(cli, argv[0]);
        else
        {
            if (scope && path.size() && snapshot.size())
            {
                if (auto_clear_bad_state)
                {
                    if (snapshot.size() && std::filesystem::exists(snapshot + "\\lock.db"))
                    {
                        std::cout << "Clearing previous bad state ( see --auto_clear_bad_state )" << std::endl;
                        std::filesystem::remove_all(snapshot);
                    }
                }

                std::cout << "Computing size of folder ( Enables percent complete, see --scope )" << std::endl;

                if (recursive)
                    _stats.direct.target = backup::recursive_delta<d8u::transform::_DefaultHash>(json, snapshot, path, [](auto name, auto size, auto time) { return true; });
                else
                    _stats.direct.target = backup::single_delta< d8u::transform::_DefaultHash>(json, snapshot, path, [](auto name, auto size, auto time) { return true;  });
            }

            if (sdomain.size())
            {
                domain = d8u::util::to_bin_sse(sdomain);
            }
            else
            {
                domain.resize(d8u::util::default_domain.size());
                std::copy(d8u::util::default_domain.begin(), d8u::util::default_domain.end(), domain.begin());
            }

            if (snapshot.size())
                std::filesystem::create_directories(snapshot);

            if (image.size())
                std::filesystem::create_directories(image);

            auto on_file = [&](auto & name, auto size, auto time)
            {
                auto pos1 = name.rfind("/");
                if (pos1 == std::string::npos)
                    pos1 = 0;

                auto pos2 = name.rfind("\\");
                if (pos2 == std::string::npos)
                    pos2 = 0;

                auto pos = (pos1 > pos2) ? pos1 : pos2;

                current_file = name.substr(pos+1);

                return true;
            };

            auto do_switch = [&](auto& store, auto _hash_t)
            {
                using hash_t = typename decltype(_hash_t)::type;

                hash_t key,result;

                if (skey.size())
                    key = d8u::util::to_bin_t<hash_t>(skey);

                switch (switch_t(action))
                {
                case switch_t("backup"):
                    if (auto_clear_bad_state)
                    {
                        if (snapshot.size() && std::filesystem::exists(snapshot + "\\lock.db"))
                        {
                            std::cout << "Clearing previous bad state ( see --auto_clear_bad_state )" << std::endl;
                            std::filesystem::remove_all(snapshot);
                        }
                    }

                    if (disable_mapping)
                    {
                        if (vss)
                        {
#ifdef _WIN32
                            if (recursive)
                            {
                                std::cout << "VSS Recursive Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::vss_folder2<false, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, max_memory * 1024 * 1024,sequence, index);
                            }
                            else
                            {
                                std::cout << "VSS Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::vss_single2<false, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, max_memory * 1024 * 1024,sequence, index);
                            }
#else
                            std::cout << "VSS Not available on non-windows platform." << std::endl;
#endif
                        }
                        else
                        {
                            if (recursive)
                            {
                                std::cout << "Recursive Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::recursive_folder2<false, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, "", 0, max_memory * 1024 * 1024,sequence, index);
                            }
                            else
                            {
                                std::cout << "Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::single_folder2<false, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, "", 0, max_memory * 1024 * 1024,sequence, index);
                            }
                        }
                    }
                    else
                    {

                        if (vss)
                        {
#ifdef _WIN32
                            if (recursive)
                            {
                                std::cout << "VSS Recursive Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::vss_folder2<true, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, max_memory * 1024 * 1024,sequence, index);
                            }
                            else
                            {
                                std::cout << "VSS Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::vss_single2<true, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, max_memory * 1024 * 1024,sequence, index);
                            }
#else
                            std::cout << "VSS Not available on non-windows platform." << std::endl;
#endif
                        }
                        else
                        {
                            if (recursive)
                            {
                                std::cout << "Recursive Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::recursive_folder2<true, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, "", 0, max_memory * 1024 * 1024,sequence, index);
                            }
                            else
                            {
                                std::cout << "Directory Backup: " << path << "; State: " << snapshot << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;
                                result = backup::single_folder2<true, hash_t>(json, snapshot, _stats, path, store, on_file, domain, files, 1024 * 1024, threads, compression, block_grouping, 128 * 1024 * 1024, "", 0, max_memory * 1024 * 1024,sequence, index);
                            }
                        }
                    }

                    pk = d8u::util::to_hex(result);

                    break;
                case switch_t("validate"):

                    std::cout << "Validate Directory: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    if (validate::folder2<hash_t>(_stats,key, store, domain, 1024 * 1024, 128 * 1024 * 1024, threads, files))
                        std::cout << "Validation Success" << std::endl;
                    else
                        std::cout << "Error Detected" << std::endl;
                    break;
                case switch_t("validate_deep"):

                    std::cout << "Validate Directory Deep: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    if (validate::deep_folder2<hash_t>(_stats,key, store, domain, 1024 * 1024, 128 * 1024 * 1024, threads, files))
                        std::cout << std::endl << "Validation Success" << std::endl;
                    else
                        std::cout << std::endl << "Error Detected" << std::endl;
                    break;
                case switch_t("delta"):

                    running = false;
                    console.join();

                    if (recursive)
                        backup::recursive_delta<hash_t>(json,snapshot, path, [](auto name, auto size, auto time)
                        {
                            std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                            return true;
                        });
                    else
                        backup::single_delta<hash_t>(json,snapshot, path, [](auto name, auto size, auto time)
                        {
                            std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                            return true;
                        });

                    break;
                case switch_t("search"):
                {
                    std::cout << "Search: " << path << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(key,store,domain,validate);

                    running = false;
                    console.join();

                    auto count = handle.Search(path, [&](auto size, auto time, auto name, auto keys)
                    {
                        std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                        return true;
                    });

                    std::cout << "Found " << count << " files" << std::endl << std::endl;

                    handle.PrintUsage();
                }
                    break;
                case switch_t("restore"):

                    std::cout << "Restore: " << path << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    std::filesystem::create_directories(path);

                    restore::folder2(_stats, path, key, store, domain, validate, validate, 1024 * 1024, 128* 1024 * 1024, threads, files);

                    break;
                case switch_t("fetch"):
                {
                    std::cout << "Fetch: " << path << " >> " << dest << "; Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(key, store, domain, validate);

                    running = false;
                    console.join();

                    handle.Fetch(path,dest ,threads);

                    handle.PrintUsage();
                }
                    break;
                case switch_t("enumerate"):
                {
                    std::cout << "Enumerate: " << " Domain: " << d8u::util::to_hex(domain) << std::endl << std::endl;

                    mount::Path handle(key, store, domain, validate);

                    running = false;
                    console.join();

                    handle.Enumerate([&](auto size, auto time, auto name, auto keys)
                    {
                        std::cout << name << " " << size << " bytes " << time << " changed" << std::endl;

                        return true;
                    });

                    handle.PrintUsage();
                }
                    break;
                }
            };

            bool simple_command = false;

            auto simple = [&](auto _hash_t)
            {
                using hash_t = typename decltype(_hash_t)::type;

                hash_t key;

                if (skey.size())
                    key = d8u::util::to_bin_t<hash_t>(skey);

                switch (switch_t(action))
                {
                case switch_t("rng_validate"):
                {
                    std::filesystem::create_directories(snapshot);
                    volrng::volume::Test<volrng::DISK> handle(snapshot);

                    std::cout << "--rng_validate " << snapshot << " " << path << std::endl;
                    if (handle.Validate(path))
                        std::cout << "Point in Time Valid" << std::endl;
                    else
                        std::cout << "Point in Time INVALID" << std::endl;
                }
                break;
                case switch_t("rng_step"):
                {
                    std::filesystem::create_directories(snapshot);
                    volrng::volume::Test<volrng::DISK> handle(snapshot);

                    handle.Dismount();

                    std::cout << "--rng_step " << snapshot << " " << path << " " << 100 << "mb" << std::endl;
                    handle.Run(100 * 1024 * 1024, path);
                    std::cout << "success" << std::endl;
                }
                    break;
                case switch_t("rng_mount"):
                {
                    std::filesystem::create_directories(snapshot);
                    volrng::volume::Test<volrng::DISK> handle(snapshot);

                    std::cout << "--rng_mount " << snapshot << " " << path << std::endl;
                    handle.Mount(path);
                    std::cout << "success" << std::endl;
                }
                    break;
                case switch_t("rng_dismount"):
                {
                    std::filesystem::create_directories(snapshot);
                    volrng::volume::Test<volrng::DISK> handle(snapshot);

                    std::cout << "--rng_dismount " << snapshot << std::endl;
                    handle.Dismount();
                    std::cout << "success" << std::endl;
                }
                    break;

                case switch_t("list"):
                    if (password.size() && image.size() && snapshot.size())
                    {
                        std::cout << "Listing image metadata..." << std::endl;

                        kreg::LocalGroup group(snapshot + "\\group", password, image);

                        group.EnumerateStream([&](auto & point_in_time) 
                        {
                            std::cout << point_in_time << std::endl << std::endl;
                        });
                    }
                    else if (password.size() && host.size() && snapshot.size())
                    {
                        std::cout << "Listing store metadata..." << std::endl;

                        kreg::Group group(snapshot + "\\group", password, host + ":7007");

                        group.EnumerateStream([&](auto& point_in_time)
                        {
                            std::cout << point_in_time << std::endl << std::endl;
                        });
                    }
                    simple_command = true;
                    break;
                case switch_t("diagnose"):

                    running = false;

                    console.join();

                    diagnose::self_diagnose();

                    simple_command = true;
                    break;
                case switch_t("compare"):

                    std::cout << "Compare: " << path << " " << host << std::endl << std::endl;

                    if (d8u::compare::folders(path, host, files))
                        std::cout << "Match!" << std::endl;
                    else
                        std::cout << "DIFFERENT" << std::endl;

                    simple_command = true;
                    break;
                case switch_t("sync"):
                {
                    std::cout << "Sync: " << image << " " << host << std::endl << std::endl;

                    if (aux_hash)
                    {
                        blocksync::Sync<hash_t, volstore::Image<fast_hash>, volstore::BinaryStoreClient> sync_handle(snapshot, image, host);

                        sync_handle.Push(_stats, validate, threads);
                    }
                    else
                    {
                        blocksync::Sync<hash_t, volstore::Image<d8u::transform::_DefaultHash>, volstore::BinaryStoreClient> sync_handle(snapshot, image, host);

                        sync_handle.Push(_stats, validate, threads);
                    }

                    simple_command = true;
                }   break;
                case switch_t("migrate"):
                {
                    std::cout << "Migrate: " << image << " " << skey << " " << host << std::endl << std::endl;

                    if (aux_hash)
                    {
                        blocksync::Sync<hash_t, volstore::Image<fast_hash>, volstore::BinaryStoreClient> sync_handle(snapshot, image, host);

                        sync_handle.MigrateFolder(_stats, key, domain, validate, files, threads);
                    }
                    else
                    {
                        blocksync::Sync<hash_t, volstore::Image<d8u::transform::_DefaultHash>, volstore::BinaryStoreClient> sync_handle(snapshot, image, host);

                        sync_handle.MigrateFolder(_stats, key, domain, validate, files, threads);
                    }

                    simple_command = true;
                }   break;
                default: break;
                }
            };

            if (aux_hash)
                simple(tag_t<fast_hash>());
            else
                simple(tag_t<d8u::transform::_DefaultHash>());

            if (storage_server)
            {
                if (auto_clear_bad_state)
                {
                    if (path.size() && std::filesystem::exists(path + "\\lock.db"))
                    {
                        std::cout << "Clearing previous bad server state ( see --auto_clear_bad_state )" << std::endl;
                        std::filesystem::remove(path + "\\lock.db");
                    }
                }

                if (aux_hash)
                {
                    StorageService2< fast_hash> service(path, threads, hport, qport, rport, wport);
                    pservice1 = &service;

                    pstats = service.Stats();

                    service.Join();
                }
                else
                {
                    StorageService2< d8u::transform::_DefaultHash> service(path, threads, hport, qport, rport, wport);
                    pservice2 = &service;

                    pstats = service.Stats();

                    service.Join();
                }
            }
            else if(!simple_command)
            {
                if (image.size())
                {
                    if (auto_clear_bad_state)
                    {
                        if (image.size() && std::filesystem::exists(image + "\\lock.db"))
                        {
                            std::cout << "Image was locked, forcing unlock ( see --auto_clear_bad_state )" << std::endl;
                            std::filesystem::remove(image + "\\lock.db");
                        }
                    }                

                    if (aux_hash)
                    {
                        volstore::Image2< fast_hash> store(image);
                        do_switch(store, tag_t<fast_hash>());
                    }
                    else
                    {
                        volstore::Image2< d8u::transform::_DefaultHash> store(image);
                        do_switch(store, tag_t<d8u::transform::_DefaultHash>());
                    }

                    if (pk.size() && password.size() && image.size() && snapshot.size())
                    {
                        std::cout << "Inserting metadata..." << std::endl;

                        kreg::LocalGroup group(snapshot + "\\group", password,image);

                        group.AddElement(pstats->String() + " " + pk + " " + description);
                    }
                }
                else
                {
                    std::string query, read, write;

                    switch (switch_t(action))
                    {
                    default:
                    case switch_t("delta"):
                        break;
                    case switch_t("backup"):
                        query = host + ":" + qport;
                        write = host + ":" + wport;
                        break;
                    case switch_t("fetch"):
                    case switch_t("enumerate"):
                    case switch_t("search"):
                    case switch_t("validate_deep"):
                    case switch_t("restore"):
                        read = host + ":" + rport;
                        break;
                    case switch_t("validate"):
                        query = host + ":" + qport;
                        read = host + ":" + rport;
                        break;
                    }

                    if (auto_clear_bad_state)
                    {
                        if (snapshot.size() && std::filesystem::exists(snapshot + "\\lock.db"))
                        {
                            std::cout << "Clearing previous bad state ( see --auto_clear_bad_state )" << std::endl;
                            std::filesystem::remove_all(snapshot);
                        }
                    }

                    volstore::BinaryStoreClient2<> store(snapshot + "\\" + host + ".cache", query, read, write, net_buffer * 1024 * 1024);
                    pclient = &store;
                    
                    if (aux_hash)
                        do_switch(store, tag_t<fast_hash>());
                    else
                        do_switch(store, tag_t<d8u::transform::_DefaultHash>());

                    if (pk.size() && password.size() && host.size() && snapshot.size())
                    {
                        std::cout << "Inserting metadata..." << std::endl;

                        kreg::Group group(snapshot + "\\group", password, host + ":7007");

                        group.AddElement(pstats->String() + " " + pk + " " + description);
                    }
                }
            }
        }
    }
    catch (const std::exception & ex)
    {
        std::cerr << std::endl << ex.what() << std::endl;
        return -1;
    }

    if (running)
    {
        running = false;
        console.join();
    }

    pstats->Print();

    if(pk.size()) std::cout << std::endl << std::endl << "Key: " << pk << std::endl << std::endl;

    std::chrono::duration<double> elapsed = std::chrono::high_resolution_clock::now() - start;
    uint64_t seconds = elapsed.count();

    std::cout << std::endl << "Finished in: " << (seconds / 60) << "m " << (seconds % 60) << "s" << std::endl;

    return 0;
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


