/*
 * Hasher.cpp
 *
 *  Created on: May 23, 2020
 *      Author: adam
 */

#include "Hasher.h"

#include <exception>
#include <fstream>
#include <iomanip>
#include <ostream>
#include <stdexcept>
#include <sstream>
#include <cstring>

using std::ifstream;
using std::string;
using std::ostream;
using std::hex;
using std::setw;
using std::setfill;
using std::runtime_error;
using std::stringstream;
using std::memcpy;
typedef Hasher::hash hash;

Hasher Hasher::hasher;

ostream& operator<<(ostream & out, const Hash & in) {
	XXH64_canonical_t canonical;
	XXH64_canonicalFromHash(&canonical, in.hash_data);

	std::ios::fmtflags flags = out.flags();
	char fill = out.fill();
	out<<hex<<setfill('0');

	for (int i=0; i<8; i++){
		out<<setw(2)<<(unsigned int) canonical.digest[i];
	}

	out.setf(flags);
	out.fill(fill);

	return out;
}

Hash::Hash() :
	hash_data(0) {
}

Hash::Hash(const XXH64_hash_t input_hash):
	hash_data(input_hash) {
}

Hash::Hash(const char (&data) [HASH_LENGTH])	{
	XXH64_canonical_t canonical;
	memcpy(canonical.digest, data, HASH_LENGTH);
	hash_data = XXH64_hashFromCanonical(&canonical);
}

void Hash::getBinHash(char (&data) [HASH_LENGTH]) const {
	bin_hash canonical;
	XXH64_canonicalFromHash(&canonical, hash_data);

	memcpy(data, canonical.digest, HASH_LENGTH);
}

Hash::operator string() const {
	stringstream sbuilder;

	XXH64_canonical_t canonical;
	XXH64_canonicalFromHash(&canonical, hash_data);

	sbuilder<<hex<<setfill('0');

	for (int i=0; i<8; i++){
		sbuilder<<setw(2)<<(unsigned int) canonical.digest[i];
	}

	return sbuilder.str();
}

bool Hash::operator<(const Hash &b) const {
	return hash_data<b.hash_data;
}

bool Hash::operator!=(const Hash &b) const {
	return hash_data!=b.hash_data;
}

bool Hash::operator==(const Hash &b) const {
	return hash_data==b.hash_data;
}

size_t Hash::length() const{
	return HASH_LENGTH;
}

Hasher::Hasher() :
	buffer(new char [HASHER_BUFFER_SIZE]),
	state(XXH64_createState()) {
	if (buffer == NULL || state==NULL)
		throw runtime_error("Unable to initialize Hasher");
}

Hasher & Hasher::getHasher() {
	return hasher;
}

Hasher::~Hasher() {
	XXH64_freeState(state);
	delete [] buffer;
}

hash Hasher::hashFromFile(string const & filename) {
	if (XXH64_reset(state, HASHER_SEED) != 0)
			throw runtime_error("Unable to reset XXH64");

	ifstream input(filename);

	if (! input.is_open())
		throw runtime_error("Unable to open");

	while (input.good()) {

		input.read(buffer, HASHER_BUFFER_SIZE);

		if (XXH64_update(state, buffer, input.gcount()) != 0) {
				throw runtime_error("Unable to update XXH64");
		}
	}

	return XXH64_digest(state);
}

hash Hasher::hashFromBytes(char const * stream, size_t length) {
	return XXH64(stream, length, HASHER_SEED);
}
