/*
 * Hasher.h
 *
 *  Created on: May 23, 2020
 *      Author: adam
 */

#ifndef HASHER_H_
#define HASHER_H_

#include <xxhash.h>
#include <string>
#include <ostream>

#define HASH_LENGTH sizeof(XXH64_hash_t)
#define HASHER_SEED 0
#define HASHER_BUFFER_SIZE 4*1024*1024

class Hash {
	typedef XXH64_canonical_t bin_hash;
private:
	XXH64_hash_t hash_data;
public:
	Hash();

	Hash(const XXH64_hash_t input_hash);

	Hash(const char (&data) [HASH_LENGTH]);

	void getBinHash(char (&data) [HASH_LENGTH]) const;

	operator std::string() const;

	friend std::ostream& operator<<(std::ostream & out, const Hash & in);

	bool operator<(const Hash &b) const;

	bool operator!=(const Hash &b) const;

	bool operator==(const Hash &b) const;

	size_t length() const;
};

class Hasher {
public:
	typedef Hash hash;

private:
	static Hasher hasher;
	char * const buffer;
	XXH64_state_t* const state;
	Hasher();
public:
	static Hasher &getHasher();

	virtual ~Hasher();

	hash hashFromFile(std::string const & filename);

	hash hashFromBytes(char const * stream, size_t length);
};

#endif /* HASHER_H_ */
