/*
 * File.h
 *
 *  Created on: May 24, 2020
 *      Author: adam
 */

#ifndef FILE_H_
#define FILE_H_

#include <ctime>
#include <map>
#include <ostream>
#include <string>


#include "Database.h"
#include "Hasher.h"
#include "HashStore.h"

class DedupeError;
class HashStore;

class File {
public:
	static Hash getFiemapHash(std::string const & filename,
			size_t size);
private:
	HashStore &hs;

	const std::string * filename;
	std::time_t mTime;
	Hasher::hash extentsHash;
	Hasher::hash dataHash;

	size_t size;
	bool clean;
	bool newFile;
public:
	File(HashStore &hs,
			std::time_t mTime,
			std::size_t size);

	const std::string & getFilename() const;

	void setFilename(const std::string & filename);

	std::time_t getMTime() const;

	size_t getSize() const;

	const Hash & getExtentsHash() const;

	const Hash & getDataHash() const;

	int operator<(const File & b) const;

	void update(std::time_t mTime,
			const Hasher::hash & extentsHash,
			const Hasher::hash & dataHash);

	void update(bool updateExtents);

	void dedupe(const std::set <File *> & dests,
			std::map <std::string, std::string> & failures);

	friend std::ostream & operator<<(std::ostream & out,
			const File & file);

	friend void HashStore::addFileFromDb(File *);

	friend void HashStore::newFile(File *);

	friend void HashStore::insertHash(File *);

	friend void HashStore::insertHashOnly(File *);

	friend void Database::insertFile(const File *);

	friend void Database::updateFile(const File *);
};

#endif /* FILE_H_ */
