/*
 * Database.h
 *
 *  Created on: May 23, 2020
 *      Author: adam
 */

#ifndef DATABASE_H_
#define DATABASE_H_

#include <sqlite3.h>

#include <ctime>
#include <list>
#include <map>
#include <set>
#include <string>

class File;
class Hash;
class HashStore;

class Database {
private:
	sqlite3 * conn;

	sqlite3_stmt * upsertHashStmt;
	sqlite3_stmt * insertFileStmt;
	sqlite3_stmt * updateFileStmt;
	sqlite3_stmt * removeHashStmt;
	sqlite3_stmt * removeFileStmt;
	sqlite3_stmt * getFilesStmt;

	sqlite3_stmt * prepareStatement(
			const std::string & query);

	void reset(sqlite3_stmt * statement);

	void bind(sqlite3_stmt * statement, int index,
			const char * raw_data, size_t length);

	template<size_t S>
	void bind(sqlite3_stmt * statement, int index,
			const char (&raw_data) [S]) {
		bind(statement, index, raw_data, S);
	}

	void bind(sqlite3_stmt * statement, int index,
			const std::string & data);

	void bind(sqlite3_stmt * statement, int index,
			std::time_t data);

	bool step(sqlite3_stmt * statement);

	void blobColumn(sqlite3_stmt * statement, int index,
			char * raw_data, size_t length);

	template<size_t S>
	void blobColumn(sqlite3_stmt * statement, int index,
			char (&raw_data) [S]) {
		blobColumn(statement, index, raw_data, S);
	}

	std::string stringColumn(sqlite3_stmt * statement,
			int index);

	std::time_t timeColumn(sqlite3_stmt * statement,
			int index);

	void finalize(sqlite3_stmt * statement);

	void executeQuery(const std::string & query);

	void removeHash(const Hash & extentsHash);
public:
	Database(const std::string & filename);
	virtual ~Database();

	void beginTransaction();

	void endTransaction();

	void upsertHash(const Hash & extentsHash,
			const Hash & dataHash);

	void insertFile(const File * file);

	void updateFile(const File * file);

	void removeFile(const std::string & filename);

	void updateFiles(std::map <std::string, File> & files,
			std::list<std::string> & ignored,
			const std::set<std::string> & patterns);

	void cleanHashes();
};

#endif /* DATABASE_H_ */
