/*
    DagDB - A lightweight structured database system.
    Copyright (C) 2012  B.J. Conijn <bcmpinc@users.sourceforge.net>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <CUnit/CUnit.h>
#include <string.h>
#include <sys/stat.h>

// include the entire file being tested.
#include "../src/base.c"

/** \file
 * Tests the I/O base system of DagDB.
 * Note that, contrary to common unit testing practice,
 * the order of the tests is important.
 * For example: tests performing reads and writes must be between a
 * test that opens the database and a test that closes it.
 * This means that if one test fails, subsequent tests are more likely to
 * fail as well.
 * Also, tests are listed in increasing complexity, such that later tests
 * can rely on functionality in previous tests to be correct, but are not
 * affected by bugs that can trigger later tests.
 */

#define PRINT_ERROR_IF_ANY if(dagdb_errno>0){fflush(stdout); fprintf(stderr,"\nerror %d: %s\n", dagdb_errno, dagdb_last_error()); dagdb_errno=0; fflush(stderr);}
#define CLEAR_ERROR dagdb_errno=0;

/**
 * Verifies that all linked lists in the free chunk table are properly linked.
 * These lists are cyclic, hence the 'last' element in the list is also the element we start with:
 * the element that is in the free chunk table.
 */
static void verify_chunk_table() {
	int_fast32_t i;
	for (i=0; i<CHUNK_TABLE_SIZE; i++) {
		dagdb_pointer list = CHUNK_TABLE_LOCATION(i);
		dagdb_pointer current = list;
		do {
			dagdb_pointer next = LOCATE(FreeMemoryChunk, current)->next;
			CU_ASSERT(current < global.size);
			CU_ASSERT_EQUAL(current, LOCATE(FreeMemoryChunk, next)->prev);
		} while (current>=HEADER_SIZE);
		CU_ASSERT_EQUAL(current, list);
	}
}

///////////////////////////////////////////////////////////////////////////////

static void print_info() {
	MemorySlab a;
	printf("memory slab: %ld entries, %lub used, %lub bitmap, %ldb wasted\n", BITMAP_SIZE, sizeof(a.data), sizeof(a.bitmap), SLAB_SIZE - sizeof(MemorySlab));
}

static void test_no_error() {
	CU_ASSERT_EQUAL(dagdb_errno, DAGDB_ERROR_NONE);
	CU_ASSERT_NOT_EQUAL(dagdb_last_error(), NULL);
}

static void test_round_up() {
	const dagdb_size L = sizeof(FreeMemoryChunk);
	CU_ASSERT_EQUAL(dagdb_round_up(0), L);
	CU_ASSERT_EQUAL(dagdb_round_up(1), L);
	CU_ASSERT_EQUAL(dagdb_round_up(2), L);
	CU_ASSERT_EQUAL(dagdb_round_up(3), L);
	CU_ASSERT_EQUAL(dagdb_round_up(4), L);
	CU_ASSERT_EQUAL(dagdb_round_up(L+0), L);
	CU_ASSERT_EQUAL(dagdb_round_up(L+1), L+S);
	CU_ASSERT_EQUAL(dagdb_round_up(L+2), L+S);
	CU_ASSERT_EQUAL(dagdb_round_up(L+3), L+S);
	CU_ASSERT_EQUAL(dagdb_round_up(L+4), L+S);
	CU_ASSERT_EQUAL(dagdb_round_up(255), 256u);
	CU_ASSERT_EQUAL(dagdb_round_up(256), 256u);
	CU_ASSERT_EQUAL(dagdb_round_up(257), 256u+S);
	CU_ASSERT_EQUAL(dagdb_round_up(258), 256u+S);
	CU_ASSERT_EQUAL(dagdb_round_up(259), 256u+S);
	CU_ASSERT_EQUAL(dagdb_round_up(260), 256u+S);
}

static void test_chunk_id() {
	int i;
	for(i=1; i<10000; i++) {
		if (free_chunk_id(i)==CHUNK_TABLE_SIZE-1) {
			CU_ASSERT_EQUAL(i, MAX_CHUNK_SIZE);
			CU_ASSERT_EQUAL(i%S, 0);
			if (i!=MAX_CHUNK_SIZE) {
				printf("\nMAX_CHUNK_SIZE should be %ld*S\n", i/S);
			}
			break;
		}
		// By allocating a chunk of a given size and then freeing it, we should not be able to increase the id.
		// Additionally, freeing one byte less, should always yield a lower id.
		if (free_chunk_id(i)>=alloc_chunk_id(i+1)) {
			printf("\ni = %d\n", i);
			CU_ASSERT_FALSE_FATAL(free_chunk_id(i)>=alloc_chunk_id(i+1));
		}
		// free_chunk_id must be monotonic.
		if (free_chunk_id(i-1)>free_chunk_id(i)) {
			printf("\ni = %d\n", i);
			CU_ASSERT_FALSE_FATAL(free_chunk_id(i-1)>free_chunk_id(i));
		}
	}
}

static dagdb_key key0 = {0x01,0x23,0x45,0x67,0x89,0xab,0xcd,0xef,0,0,0,0,0,0,0,0,0x37,0xe7,0x52,0x0f};
static int nibbles[] = {
	1,0,3,2,5,4,7,6,9,8,11,10,13,12,15,14,
	0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
	7,3,7,14,2,5,15,0
};

static void test_nibble() {
	int i;
	for (i=0; i<DAGDB_KEY_LENGTH; i++)
		CU_ASSERT_EQUAL(nibble(key0,i), nibbles[i]);
}

static CU_TestInfo test_non_io[] = {
  { "print_info", print_info }, 
  { "no_errors", test_no_error },
  { "round_up", test_round_up },
  { "nibble", test_nibble },
  { "chunk_id", test_chunk_id },
  CU_TEST_INFO_NULL,
};

///////////////////////////////////////////////////////////////////////////////
#define DB_FILENAME "test.dagdb"

static void test_load_init() {
	unlink(DB_FILENAME);
	int r = dagdb_load(DB_FILENAME); PRINT_ERROR_IF_ANY
	CU_ASSERT(r == 0); 
	dagdb_unload();
}

static void test_load_reload() {
	int r = dagdb_load(DB_FILENAME); PRINT_ERROR_IF_ANY
	CU_ASSERT(r == 0); 
	dagdb_unload();
}

static void test_superfluous_unload() {
	dagdb_unload();
}

static void test_load_failure() {
	unlink(DB_FILENAME);
	int r = mkdir(DB_FILENAME,0700);
	CU_ASSERT(r == 0); 
	r = dagdb_load(DB_FILENAME);  
	CU_ASSERT(r == -1); 
	CU_ASSERT_EQUAL(dagdb_errno, DAGDB_ERROR_INVALID_DB); CLEAR_ERROR
	dagdb_unload(); // <- again superfluous
	rmdir(DB_FILENAME);
}

static CU_TestInfo test_loading[] = {
  { "load_init", test_load_init },
  { "load_reload", test_load_reload },
  { "superfluous_unload", test_superfluous_unload },
  { "load_failure", test_load_failure },
  CU_TEST_INFO_NULL,
};

///////////////////////////////////////////////////////////////////////////////

static void test_mem_initial() {
	CU_ASSERT_EQUAL(global.size, SLAB_SIZE);
	verify_chunk_table();
	// TODO: add test that checks memory usage & bitmap of first slab.
}

static void test_mem_grow_much() {
	dagdb_pointer p = dagdb_malloc(MAX_SIZE); 
	CU_ASSERT_EQUAL(p, 0);
	CU_ASSERT_EQUAL(dagdb_errno, DAGDB_ERROR_DB_TOO_LARGE); CLEAR_ERROR
}

static void test_mem_growing() {
	int oldsize = global.size;
	CU_ASSERT_EQUAL(dagdb_errno, DAGDB_ERROR_NONE);
	dagdb_pointer p = dagdb_malloc(1024); PRINT_ERROR_IF_ANY
	// TODO: update this test when chunked memory management is implemented
	CU_ASSERT_FATAL(p>0);
	CU_ASSERT_EQUAL(global.size, oldsize + 1024);
	dagdb_free(p, 1024); 
	CU_ASSERT_EQUAL(global.size, oldsize);
}

static CU_TestInfo test_mem[] = {
  { "memory_initial", test_mem_initial },
  { "memory_error_grow", test_mem_grow_much },
  { "memory_growing", test_mem_growing },
  CU_TEST_INFO_NULL,
};

///////////////////////////////////////////////////////////////////////////////

// The keys are 1 byte too short, as the last byte is filled by the 0-termination character.
static dagdb_key key1 = {"0123456789012345678"};
static dagdb_key key2 = {"0123056789012345678"};
static dagdb_key key3 = {"0123456789012345670"};
static dagdb_key key4 = {"1123456789012345670"};

static void test_data() {
	const char *data = "This is a test";
	uint_fast32_t len = strlen(data);
	dagdb_pointer p = dagdb_data_create(len, data);
	CU_ASSERT_EQUAL(dagdb_get_type(p), DAGDB_TYPE_DATA);
	CU_ASSERT_EQUAL(dagdb_data_length(p), len);
	CU_ASSERT_NSTRING_EQUAL((const char *) dagdb_data_read(p), data, len);
	dagdb_data_delete(p);
	CU_ASSERT_EQUAL(dagdb_data_length(p), 0u); // UNDEFINED BEHAVIOR
}

static void test_element() {
	dagdb_pointer el = dagdb_element_create(key1, 1000, 1337);
	CU_ASSERT(el);
	CU_ASSERT_EQUAL(dagdb_get_type(el), DAGDB_TYPE_ELEMENT);
	CU_ASSERT_EQUAL(dagdb_element_data(el), 1000u);
	CU_ASSERT_EQUAL(dagdb_element_backref(el), 1337u);
	key k = obtain_key(el);
	CU_ASSERT_NSTRING_EQUAL(k,key1,DAGDB_KEY_LENGTH);
	dagdb_element_delete(el);
}

static void test_kvpair() {
	// Depends on element
	dagdb_pointer el = dagdb_element_create(key1, 1, 2);
	CU_ASSERT_EQUAL(dagdb_get_type(el), DAGDB_TYPE_ELEMENT);
	dagdb_pointer kv = dagdb_kvpair_create(el, 42);
	CU_ASSERT(kv);
	CU_ASSERT_EQUAL(dagdb_get_type(kv), DAGDB_TYPE_KVPAIR);
	CU_ASSERT_EQUAL(dagdb_kvpair_key(kv), el);
	CU_ASSERT_EQUAL(dagdb_kvpair_value(kv), 42u);
	key k = obtain_key(kv);
	CU_ASSERT_NSTRING_EQUAL(k,key1,DAGDB_KEY_LENGTH);
	dagdb_kvpair_delete(kv);
	CU_ASSERT_EQUAL(dagdb_element_data(el), 1u);
	CU_ASSERT_EQUAL(dagdb_element_backref(el), 2u);
	dagdb_element_delete(el);
}

static void test_trie() {
	dagdb_pointer t = dagdb_trie_create();
	CU_ASSERT(t);
	CU_ASSERT_EQUAL(dagdb_get_type(t), DAGDB_TYPE_TRIE);
	dagdb_trie_delete(t);
}

static CU_TestInfo test_basic_io[] = {
  { "data", test_data },
  { "element", test_element },
  { "kvpair", test_kvpair },
  { "trie", test_trie },
  CU_TEST_INFO_NULL,
};

///////////////////////////////////////////////////////////////////////////////

static void test_insert() {
	int r;
	dagdb_pointer el1 = dagdb_element_create(key1, 1, 2);
	dagdb_pointer el2 = dagdb_element_create(key2, 3, 4);
	dagdb_pointer el3 = dagdb_element_create(key1, 5, 6); // this is indeed the first key.
	CU_ASSERT_EQUAL(dagdb_get_type(el1), DAGDB_TYPE_ELEMENT);
	CU_ASSERT_EQUAL(dagdb_get_type(el2), DAGDB_TYPE_ELEMENT);
	CU_ASSERT_EQUAL(dagdb_get_type(el3), DAGDB_TYPE_ELEMENT);
	r = dagdb_trie_insert(dagdb_root(), el1);
	CU_ASSERT_EQUAL(r, 1);
	r = dagdb_trie_insert(dagdb_root(), el2);
	CU_ASSERT_EQUAL(r, 1);
	r = dagdb_trie_insert(dagdb_root(), el3); // checking duplicate inserts.
	CU_ASSERT_EQUAL(r, 0);
}

static void test_find() {
	dagdb_pointer el1 = dagdb_trie_find(dagdb_root(), key1);
	dagdb_pointer el2 = dagdb_trie_find(dagdb_root(), key2);
	CU_ASSERT_EQUAL(dagdb_get_type(el1), DAGDB_TYPE_ELEMENT);
	CU_ASSERT_EQUAL(dagdb_get_type(el2), DAGDB_TYPE_ELEMENT);
	CU_ASSERT_EQUAL(dagdb_element_data(el1), 1u);
	CU_ASSERT_EQUAL(dagdb_element_backref(el1), 2u);
	CU_ASSERT_EQUAL(dagdb_element_data(el2), 3u);
	CU_ASSERT_EQUAL(dagdb_element_backref(el2), 4u);
	CU_ASSERT_EQUAL(dagdb_trie_find(dagdb_root(), key3), 0u); // fail on empty spot
	CU_ASSERT_EQUAL(dagdb_trie_find(dagdb_root(), key4), 0u); // fail on key mismatch.
}

static void test_remove() {
	int r;
	r = dagdb_trie_remove(dagdb_root(), key1);
	CU_ASSERT_EQUAL(r, 1);
	r = dagdb_trie_remove(dagdb_root(), key1);
	CU_ASSERT_EQUAL(r, 0);
	r = dagdb_trie_remove(dagdb_root(), key3);
	CU_ASSERT_EQUAL(r, 0);
	r = dagdb_trie_remove(dagdb_root(), key4);
	CU_ASSERT_EQUAL(r, 0);

	dagdb_pointer el1 = dagdb_trie_find(dagdb_root(), key1);
	CU_ASSERT_EQUAL(el1, 0u);

	dagdb_pointer el2 = dagdb_trie_find(dagdb_root(), key2);
	CU_ASSERT_EQUAL(dagdb_get_type(el2), DAGDB_TYPE_ELEMENT);
	CU_ASSERT_EQUAL(dagdb_element_data(el2), 3u);
	CU_ASSERT_EQUAL(dagdb_element_backref(el2), 4u);

	r = dagdb_trie_remove(dagdb_root(), key2);
	CU_ASSERT_EQUAL(r, 1);
	el2 = dagdb_trie_find(dagdb_root(), key2);
	CU_ASSERT_EQUAL(el2, 0u);
}

static void test_trie_kvpair() {
	int r;
	dagdb_pointer el = dagdb_element_create(key1, 1, 2);
	CU_ASSERT_EQUAL(dagdb_get_type(el), DAGDB_TYPE_ELEMENT);
	dagdb_pointer kv = dagdb_kvpair_create(el, 3);
	CU_ASSERT_EQUAL(dagdb_get_type(kv), DAGDB_TYPE_KVPAIR);
	
	r = dagdb_trie_insert(dagdb_root(), kv);
	CU_ASSERT_EQUAL(r, 1);
	r = dagdb_trie_insert(dagdb_root(), el);
	CU_ASSERT_EQUAL(r, 0);

	CU_ASSERT_EQUAL(dagdb_trie_find(dagdb_root(), key1), kv);

	r = dagdb_trie_remove(dagdb_root(), key1);
	CU_ASSERT_EQUAL(r, 1);
	CU_ASSERT_EQUAL(dagdb_kvpair_key(kv), el);
	CU_ASSERT_EQUAL(dagdb_kvpair_value(kv), 3u);
	dagdb_element_delete(el);
}

static void test_trie_recursive_delete() {
	dagdb_pointer t = dagdb_trie_create();
	dagdb_trie_insert(t, dagdb_element_create(key1, 0, 2));
	dagdb_trie_insert(t, dagdb_element_create(key1, 1, 2));
	dagdb_trie_insert(t, dagdb_element_create(key2, 1, 2));
	dagdb_trie_insert(t, dagdb_element_create(key3, 1, 2));
	dagdb_trie_insert(t, dagdb_element_create(key4, 1, 2));
	dagdb_trie_delete(t);
}

static CU_TestInfo test_trie_io[] = {
  { "insert", test_insert },
  { "find", test_find },
  { "remove", test_remove },
  { "kvpair", test_trie_kvpair },
  { "recursive_delete", test_trie_recursive_delete },
  CU_TEST_INFO_NULL,
};

///////////////////////////////////////////////////////////////////////////////

static int open_new_db() {
	unlink(DB_FILENAME);
	return dagdb_load(DB_FILENAME);
}

static int close_db() {
	dagdb_unload();
	return 0;
}

CU_SuiteInfo base2_suites[] = {
	{ "base-non-io",   NULL,        NULL,     test_non_io },
	{ "base-loading",  NULL,        NULL,     test_loading },
	{ "base-memory",   open_new_db, close_db, test_mem },
	{ "base-basic-io", open_new_db, close_db, test_basic_io },
	{ "base-trie-io",  open_new_db, close_db, test_trie_io },
	CU_SUITE_INFO_NULL,
};
