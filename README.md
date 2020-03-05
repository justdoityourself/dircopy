# dircopy

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
