/*
 * HashStore.cpp
 *
 *  Created on: May 24, 2020
 *      Author: adam
 */

#include <stdexcept>

#include "Database.h"
#include "File.h"
#include "Hasher.h"
#include "HashStore.h"

using std::logic_error;
using std::map;
using std::pair;
using std::set;
using std::string;

HashStore::HashStore(Database * db) :
	db(*db) {
	// TODO Auto-generated constructor stub

}

HashStore::~HashStore() {
	// TODO Auto-generated destructor stub
}

void HashStore::addFileFromDb(File * file) {
	Hash & extentsHash = file->extentsHash;

	ExtentsInfo & stored = byExtents[extentsHash];
	if (stored.files.empty()) {
		stored.dataHash=file->dataHash;
		byData[stored.dataHash].insert(extentsHash);
	}

	stored.files.insert(file);
}

void HashStore::newFile(File * file) {
	Hash & extentsHash = file->extentsHash;

	ExtentsInfo & stored = byExtents[extentsHash];
	if (stored.files.empty()) {
		stored.dataHash=Hasher::getHasher()
		.hashFromFile(file->getFilename());;
		byData[stored.dataHash].insert(extentsHash);
		db.upsertHash(extentsHash, stored.dataHash);
	}

	stored.files.insert(file);

	db.insertFile(file);
}

void HashStore::insertHash(File * file) {
	Hash & extentsHash = file->extentsHash;
	ExtentsInfo & stored = byExtents[extentsHash];
	if (stored.files.empty()) {
		stored.dataHash=Hasher::getHasher()
			.hashFromFile(file->getFilename());
		byData[stored.dataHash].insert(extentsHash);

		db.upsertHash(extentsHash, stored.dataHash);
	}

	stored.files.insert(file);
	db.updateFile(file);
}

void HashStore::insertHashOnly(File * file) {
	Hash & extentsHash = file->extentsHash;
	ExtentsInfo & stored = byExtents[extentsHash];
	if (stored.files.empty()) {
		stored.dataHash=file->dataHash;
		byData[stored.dataHash].insert(extentsHash);

		db.upsertHash(extentsHash, stored.dataHash);
	}

	stored.files.insert(file);
	db.updateFile(file);
}

bool HashStore::hasExtentsHash(const Hash & extentsHash) const {
	return byExtents.count(extentsHash);
}

Hash HashStore::getDataHash(const Hash & extentsHash) const {
	return byExtents.find(extentsHash)->second.dataHash;
}

void HashStore::removeExtentsHash(File * file,
		const Hash & extentsHash) {
	ExtentsInfo & stored = byExtents.find(extentsHash)
			->second;

	stored.files.erase(file);

	if (stored.files.empty()) {
		set <Hash> & storedData=byData.find(stored.dataHash)
				->second;
		storedData.erase(extentsHash);
		if (storedData.empty()) {
			byData.erase(stored.dataHash);
		}

		byExtents.erase(extentsHash);
	}
}

void HashStore::updateFileExtentsOnly(File * file, const Hash & oldExtentsHash) {
	removeExtentsHash(file, oldExtentsHash);
	insertHashOnly(file);
}

void HashStore::removeFile(File * file) {
	removeExtentsHash(file, file->getExtentsHash());

	db.removeFile(file->getFilename());
}

void HashStore::getDedupeCandidates(map<File *, set<File *>> &
		candidates) const {
	//For each dataHash:
	for (auto & entry : byData) {
		auto & extentsHashes = entry.second;
		//If more than one extentsHashes:
		if (extentsHashes.size() > 1) {
			size_t max = 0;
			Hash ref_extentsHash;
			File * ref_file;

			//Which extentsHash has more files?
			for (auto & extentsHash : extentsHashes) {
				auto & files = byExtents.find(extentsHash)->second.files;
				size_t count = files.size();
				if (count > max) {
					max = count;
					ref_file=*files.begin();
					ref_extentsHash=extentsHash;
				}
			}

			auto & duplicates = candidates[ref_file];

			//Include every file with different extentsHash
			for (Hash extentsHash : extentsHashes) {
				if (extentsHash != ref_extentsHash) {
					for (auto & file : byExtents.find(extentsHash)->second.files) {
						duplicates.insert(file);
					}
				}
			}
		}
	}
}
