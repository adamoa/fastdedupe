/*
 * main.cpp
 *
 *  Created on: May 17, 2020
 *      Author: adam
 */
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <stdexcept>

#include "Database.h"
#include "File.h"
#include "FilesystemHelper.h"
#include "Hasher.h"
#include "HashStore.h"

typedef Hasher::hash hash;

using std::cerr;
using std::cout;
using std::exception;
using std::ifstream;
using std::invalid_argument;
using std::list;
using std::map;
using std::ofstream;
using std::runtime_error;
using std::set;
using std::string;
using std::vector;

#define BUFFER_SIZE 500

Database * db;
HashStore *hs;

int showError(const string & program) {
	cout<<"Usage: "<<program<<
			"[--db-file file] [--update-extents] [--dedupe] "
			"[--input input] [--recursive] file1 file2\n";
	return 1;
}

void listFiles(set<string> &patterns,
		map<string, File> &files,
		const set<string> &filenames,
		bool recursive) {
	FilesystemHelper &fsHelper = FilesystemHelper::getFilesystemHelper();
	list<FileInfo> fileinfos;
	list<IgnoredFile> ignored;
	cout << "Loading files...\n";
	if (recursive) {
		fsHelper.recursiveRead(filenames, fileinfos, patterns, ignored);
	} else {
		fsHelper.fetchInfos(filenames, fileinfos, patterns, ignored);
	}
	for (const auto &item : ignored) {
		cout << "Ignored '" << item.fullpath << "': " << item.message << "\n";
		if (item.error == ENOENT) {
			db->removeFile(item.fullpath);
		}
	}

	for (auto it = fileinfos.begin();
			it!=fileinfos.end();
			fileinfos.erase(it), it=fileinfos.begin()) {

		if (it->regType) {
			const auto &entry = files.emplace(it->filename,
						File(*hs, it->mTime, it->size));
			entry.first->second.setFilename(entry.first->first);
		}
	}
}

void updateFiles(map<string, File> &files,
		const set<string> &patterns,
		bool updateExtentsFlag) {

	list<string> dbIgnored;
	cout << "Reading from database...\n";
	db->updateFiles(files, dbIgnored, patterns);

	for (auto iterator = dbIgnored.begin();
			iterator != dbIgnored.end();
			dbIgnored.pop_front(), iterator=dbIgnored.begin()) {

		const string & filename = *iterator;

		cout<<"Cleaning removed file '"
				<<filename<<"'\n";

		db->removeFile(filename);
	}

	auto it = files.begin();
	while (it != files.end()) {
		auto cur = it;
		it++;

		try {
			cur->second.update(updateExtentsFlag);
			cout << cur->second;
		} catch (invalid_argument &error) {
			cout << "Ignoring file '" << cur->first << "': "
					<< error.what()
					<< "\n";
			files.erase(cur);
		}
	}

	db->cleanHashes();
}

void doDedupe() {
	cout << "\nFetching duplicates...\n";
	map<File*, set<File*> > candidates;
	hs->getDedupeCandidates(candidates);
	if (candidates.size())
		cout << "\nDeduplicating:\n";
	else
		cout << "\nNo duplicates pending deduplication\n";

	for (auto &entry : candidates) {
		cout << entry.first->getFilename() << "\n";
		for (auto &file : entry.second) {
			cout << "  " << file->getFilename() << "\n";
		}
		map<string, string> failures;
		try {
			entry.first->dedupe(entry.second, failures);
		} catch (exception &error) {
			cerr << "Could not dedupe: " << error.what() << "\n";
		}
		for (auto &failure : failures) {
			cout << "Could not dedupe '" << failure.first << "': "
					<< failure.second << "\n";
		}
		cout << "\n";
	}
}

int process(const string & db_file,
		const set<string> & filenames, bool recursive,
		bool updateExtentsFlag, bool dedupe) {

	db = new Database(db_file);
	hs = new HashStore(db);

	set<string> patterns;
	map<string, File> files;

	listFiles(patterns, files, filenames, recursive);

	updateFiles(files, patterns, updateExtentsFlag);

	if (dedupe) doDedupe();

	delete hs;
	delete db;

	return 0;
}

void readInput(const string & input_filename,
		set<string> & files) {
	ifstream input(input_filename);

	if (! (input.is_open())) {
		throw invalid_argument("Could not load input file.\n");
	}

	char * temp_filename = new char [BUFFER_SIZE];
	while (input.good()) {
		input.getline(temp_filename, BUFFER_SIZE);
		if (temp_filename[0] != '\0')
			files.emplace(temp_filename);
	}
	delete [] temp_filename;
	input.close();
}

int main(int argc, char **argv) {
	bool updateExtents=false,
			error=false,
			dedupe=false,
			recursive=false,
			file_as_input=false;
	string db_file="files.db";
	string input_file;
	set <string> files;

	for (int i = 1; i<argc; i++) {
		string argument(argv[i]);
		int pending = argc -i -1;

		if (argument=="--update-extents") {
			updateExtents=true;
		} else if (argument=="--db-file") {
			if (pending >= 1) {
				db_file=argv[i+1];
				i++;
			} else {
				cerr<<"--db-file requires an argument.\n";
				error=true;
				break;
			}
		} else if (argument=="--dedupe") {
			dedupe=true;
		} else if (argument=="--recursive") {
			recursive=true;
		} else if (argument=="--input") {
			if (pending >= 1) {
				if (file_as_input) {
					cerr<<"Input file used more than 1 time.\n";
					error=true;
					break;
				} else {
					file_as_input=true;
					input_file=argv[i+1];
					i++;
				}
			} else {
				cerr<<"--input requires one argument.\n";
				error=true;
				break;
			}
		} else if (argument.substr(0, 1) != "-" ){
			files.insert(argument);
		} else {
			cerr<<"Unrecognized option '"<<argument<<"'.\n";
			error=true;
			break;
		}
	}

	if (file_as_input) {
		readInput(input_file, files);
	}

	if (error || files.size()==0) {
		return showError(argv[0]);
	}

	return process(db_file, files, recursive, updateExtents, dedupe);

}
