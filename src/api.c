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

#include <gcrypt.h>

#include "api.h"
#include "base.h"

typedef uint8_t dagdb_hash[DAGDB_KEY_LENGTH];
static void dagdb_data_hash(dagdb_hash h, int length, const void * data) {
	gcry_md_hash_buffer(GCRY_MD_SHA1, h, data, length);
}

static int cmphash(const void *p1, const void *p2) {
    return memcmp(p1, p2, DAGDB_KEY_LENGTH);
}

/** Applies bitwise inversion to the hash, to avoid hash collisions between data and records. */
static void flip_hash(dagdb_hash h) {
	int i;
	for (i=0; i<DAGDB_KEY_LENGTH; i++) {
		h[i]=~h[i];
	}
}

static void dagdb_record_hash(dagdb_hash h, long int entries, dagdb_record_entry * items) {
	long i;
	dagdb_hash * keylist = malloc(entries * sizeof(dagdb_key) * 2);
	for (i=0; i<entries*2; i++) {
		dagdb_element_key(keylist[i], ((dagdb_pointer*)items)[i]);
	}
	qsort(keylist, entries, DAGDB_KEY_LENGTH*2, cmphash);
	dagdb_data_hash(h, entries * 2 * DAGDB_KEY_LENGTH, keylist);
	flip_hash(h);
}

dagdb_handle dagdb_find_data(uint64_t length, const char* data) {
	dagdb_hash h;
	dagdb_data_hash(h, length, data);
	return dagdb_trie_find(dagdb_root(), h);
}

dagdb_handle dagdb_find_record(uint_fast32_t entries, dagdb_record_entry * items) {
	dagdb_hash h;
	dagdb_record_hash(h, entries, items);
	return dagdb_trie_find(dagdb_root(), h);
}

dagdb_handle dagdb_write_data(uint64_t length, const char* data) {
	dagdb_hash h;
	dagdb_data_hash(h, length, data);
	
	// Check if it already exists.
	dagdb_handle r = dagdb_trie_find(dagdb_root(), h);
	if (r) return r;
	
	dagdb_handle dataptr = 0;
	dagdb_handle backref = 0;
	dagdb_handle element = 0;
	
	// Create data, backref and element.
	dataptr = dagdb_data_create(length, data);
	if (!dataptr) goto error;
	backref = dagdb_trie_create();
	if (!backref) goto error;
	element = dagdb_element_create(h, dataptr, backref);
	if (!element) goto error;
	int res = dagdb_trie_insert(dagdb_root(), element);
	if (res<0) goto error;

	// Inserted in root trie, return handle.
	return element;
	
	error:
	if (element) dagdb_element_delete(element);
	if (backref) dagdb_trie_delete(backref);
	if (dataptr) dagdb_data_delete(dataptr);
	return 0;
}

dagdb_handle dagdb_write_record(uint_fast32_t entries, dagdb_record_entry* items) {
	dagdb_hash h;
	dagdb_record_hash(h, entries, items);
	
	// Check if it already exists.
	dagdb_handle r = dagdb_trie_find(dagdb_root(), h);
	if (r) return r;
	
	dagdb_handle trie = 0;
	dagdb_handle backref = 0;
	dagdb_handle element = 0;

	// Create the trie, backref and element
	trie = dagdb_trie_create();
	if (!trie) goto error;
	backref = dagdb_trie_create();
	if (!backref) goto error;
	element = dagdb_element_create(h, trie, backref);
	if (!element) goto error;
	int res = dagdb_trie_insert(dagdb_root(), element);
	if (res<0) goto error;
					
	long i;
	for (i=0; i<entries; i++) {
		
		// Insert in our record trie
		
	}

	// Inserted in root trie, return handle.
	return element;

	error:
	if (element) dagdb_element_delete(element);
	if (backref) dagdb_trie_delete(backref);
	if (trie) dagdb_trie_delete(trie);
	return 0;
}
