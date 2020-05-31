/*
 * File.cpp
 *
 *  Created on: May 24, 2020
 *      Author: adam
 */

#include <iomanip>
#include <map>
#include <stdexcept>
#include <vector>

#include "File.h"

#include "FilesystemHelper.h"
#include "HashStore.h"

using std::invalid_argument;
using std::left;
using std::map;
using std::right;
using std::ostream;
using std::set;
using std::setfill;
using std::setw;
using std::string;
using std::time_t;
using std::vector;

typedef Hasher::hash hash;

ostream & operator<<(ostream & out, const File & file) {
	std::ios_base::fmtflags flags=out.flags();
	char fill = out.fill();

	out
		<<setw(30)<<left<<*file.filename<<" "
		<<setw(10)<<right<<file.size<<" "
		<<setw(10)<<file.mTime<<" ";

	if (file.clean)
		out
			<<setw(HASH_LENGTH*2)<<file.dataHash<<" "
			<<setw(HASH_LENGTH*2)<<file.extentsHash<<"\n";
	else
		out
			<<setw(HASH_LENGTH*2)<<setfill('?')<<""<<" "
			<<setw(HASH_LENGTH*2)<<""<<"\n";

	out.setf(flags);
	out.fill(fill);

	return out;
}

File::File(HashStore &hs,
		time_t mTime,
		size_t size) :
	hs(hs) {
	this->mTime = mTime;
	this->size = size;
	clean = false;
	newFile = true;
	filename = NULL;
}

const string & File::getFilename() const {
	return *filename;
}

void File::setFilename(const string & filename) {
	this->filename = &filename;
}

void File::update(std::time_t mTime,
		const Hasher::hash & extentsHash,
		const Hasher::hash & dataHash) {
	newFile = false;
	if (this->mTime == mTime) {
		clean = true;
		this->extentsHash = extentsHash;
		this->dataHash = dataHash;

		hs.addFileFromDb(this);
	} else {
		clean = false;
	}
}

const Hash & File::getExtentsHash() const {
	return extentsHash;
}

const Hash & File::getDataHash() const {
	return dataHash;
}

time_t File::getMTime() const {
	return mTime;
}

size_t File::getSize() const {
	return size;
}

hash File::getFiemapHash(string const & filename, size_t size) {
	vector<FiemapData> extents =
			FilesystemHelper::getFilesystemHelper()
			.getFiemapData(filename, size);

	return Hasher::getHasher()
		.hashFromBytes(
				(char *) &*extents.begin(),
				extents.size() * sizeof(FiemapData));
}

void File::update(bool updateExtents) {
	if (!clean) {
		extentsHash=getFiemapHash(*filename, size);

		if (newFile)
			hs.newFile(this);
		else
			hs.insertHash(this);

		dataHash=hs.getDataHash(extentsHash);
		clean=true;
	} else if (updateExtents) {
		Hash oldExtentsHash = extentsHash;
		try {
			extentsHash=getFiemapHash(*filename, size);

			if (oldExtentsHash!=extentsHash)
				hs.updateFileExtentsOnly(this, oldExtentsHash);
		} catch (invalid_argument & error) {
			hs.removeFile(this);
			throw;
		}
	}
}

void File::dedupe(const set<File *> & dests,
		map<string, string> & failures) {
	set<string> filenames;

	for (File * file : dests) {
		filenames.insert(* file->filename);
	}

	FilesystemHelper::getFilesystemHelper()
	.dedupe(* filename, filenames, size, failures);

	for (File * file : dests) {
		try {
			file->update(true);
			if (file->extentsHash != extentsHash)
				if (! failures.count(* file->filename))
					failures.emplace(* file->filename,
							"Check shows not deduped");

		} catch (invalid_argument & error) {
			if (! failures.count(* file->filename))
				failures.emplace(* file->filename,
						"File removed");
		}

	}
}

int File::operator<(const File & b) const {
	return filename < b.filename;
}

