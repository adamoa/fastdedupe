/*
 * HashStore.h
 *
 *  Created on: May 24, 2020
 *      Author: adam
 */

#ifndef HASHSTORE_H_
#define HASHSTORE_H_

#include <map>
#include <set>

#include "Hasher.h"

class Database;
class File;

class HashStore {
private:
	struct ExtentsInfo {
		std::set <File *> files;
		Hash dataHash;
	};

	Database & db;

	std::map<Hash, ExtentsInfo> byExtents;
	std::map<Hash, std::set<Hash>> byData;

	void removeExtentsHash(File * file, const Hash & extentsHash);

public:
	HashStore(Database * db);

	virtual ~HashStore();

	void addFileFromDb(File * file);

	void newFile(File * file);

	void insertHash(File * file);

	void insertHashOnly(File * file);

	void insertHash(File * file,
			const Hash & extentsHash);

	bool hasExtentsHash(const Hash & extentsHash) const;

	Hash getDataHash(const Hash & extentsHash) const;

	void updateFileExtentsOnly(File * file, const Hash & oldExtentsHash);

	void removeFile(File * file);

	void getDedupeCandidates(
			std::map<File *, std::set<File *>> &
			candidates) const;
};

#endif /* HASHSTORE_H_ */
