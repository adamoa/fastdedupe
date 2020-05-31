/*
 * Database.cpp
 *
 *  Created on: May 23, 2020
 *      Author: adam
 */

#include <sqlite3.h>

#include <cstring>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>

#include "Database.h"
#include "File.h"
#include "Hasher.h"
#include "HashStore.h"

using std::list;
using std::map;
using std::memcpy;
using std::memset;
using std::runtime_error;
using std::set;
using std::string;
using std::time_t;

typedef Hasher::hash hash;

Database::Database(const string & filename) {
	int status = sqlite3_open_v2(filename.c_str(), &conn,
			SQLITE_OPEN_READWRITE |
			SQLITE_OPEN_CREATE |
			SQLITE_OPEN_NOMUTEX |
			SQLITE_OPEN_NOFOLLOW,
			NULL);
	if (status != SQLITE_OK) {
		sqlite3_close_v2(conn);
		throw runtime_error(sqlite3_errstr(status));
	}

	executeQuery("CREATE TABLE IF NOT EXISTS "
			"hashes ("
			"extents_hash BLOB PRIMARY KEY NOT NULL,"
			"data_hash BLOB)");

	executeQuery("CREATE TABLE IF NOT EXISTS "
			"files ("
			"filename VARCHAR PRIMARY KEY NOT NULL,"
			"m_time INTEGER,"
			"extents_hash BLOB REFERENCES hashes)");

	upsertHashStmt = prepareStatement("INSERT INTO hashes VALUES (?, ?) "
			"ON CONFLICT (extents_hash) DO "
			"UPDATE SET data_hash = ?2 WHERE extents_hash = ?1");

	insertFileStmt = prepareStatement("INSERT INTO files VALUES (?, ?, ?)");

	updateFileStmt = prepareStatement("UPDATE files SET "
			"(m_time, extents_hash) = (?, ?) WHERE "
			"filename = ?");

	removeHashStmt = prepareStatement("DELETE FROM hashes WHERE extents_hash = ?");

	removeFileStmt = prepareStatement("DELETE FROM files WHERE filename = ?");

	getFilesStmt = prepareStatement("SELECT filename, m_time, data_hash, extents_hash "
			"FROM files NATURAL JOIN hashes "
			"WHERE filename LIKE ? OR filename LIKE ?1||'/%'");

	this->beginTransaction();
}

Database::~Database() {
	finalize(getFilesStmt);
	finalize(removeFileStmt);
	finalize(removeHashStmt);
	finalize(updateFileStmt);
	finalize(insertFileStmt);
	finalize(upsertHashStmt);

	this->endTransaction();

	sqlite3_close_v2(conn);
}

sqlite3_stmt * Database::prepareStatement(const string & query) {
	sqlite3_stmt * statement;

	if (sqlite3_prepare_v2(conn,
				query.c_str(),
				query.length(),
				&statement,
				NULL) != SQLITE_OK)
			throw runtime_error(sqlite3_errmsg(conn));

	return statement;
}

void Database::reset(sqlite3_stmt * statement) {
	if (sqlite3_reset(statement) != SQLITE_OK)
			throw runtime_error(sqlite3_errmsg(conn));
}

void Database::bind(sqlite3_stmt * statement, int index, const char * raw_data, size_t length) {
	if (sqlite3_bind_blob64(statement, index, raw_data, HASH_LENGTH, SQLITE_STATIC) != SQLITE_OK)
		throw runtime_error(sqlite3_errmsg(conn));
}

void Database::bind(sqlite3_stmt * statement, int index, const std::string & data) {
	if (sqlite3_bind_text64(statement, index, data.c_str(), data.length(), SQLITE_STATIC, SQLITE_UTF8) != SQLITE_OK)
			throw runtime_error(sqlite3_errmsg(conn));
}

void Database::bind(sqlite3_stmt * statement, int index, std::time_t data) {
	if (sqlite3_bind_int64(statement, index, data) != SQLITE_OK)
				throw runtime_error(sqlite3_errmsg(conn));
}

bool Database::step(sqlite3_stmt * statement) {
	int status = sqlite3_step(statement);

	if (status != SQLITE_DONE && status != SQLITE_ROW) {
		reset(statement);
		throw runtime_error(sqlite3_errmsg(conn));
	}

	if (status==SQLITE_DONE)
		reset(statement);

	return status==SQLITE_ROW;
}

void Database::blobColumn(sqlite3_stmt * statement, int index, char * raw_data, size_t length) {
	const void *input = sqlite3_column_blob(statement, index);
	size_t input_length = sqlite3_column_bytes(statement, index);

	if (input == NULL) {
		if (sqlite3_errcode(conn) == SQLITE_NOMEM)
			throw runtime_error(sqlite3_errmsg(conn));
		else {
			memset(raw_data, 0, length);
		}
	} else {
		if (input_length != length)
			throw runtime_error("Data size mismatch");

		memcpy(raw_data, input, length);
	}
}

string Database::stringColumn(sqlite3_stmt * statement, int index) {
	const char * text = (const char *) sqlite3_column_text(statement, index);

	if (text == NULL) {
		if (sqlite3_errcode(conn) == SQLITE_NOMEM)
			throw runtime_error(sqlite3_errmsg(conn));
		else {
			string result;
			return "";
		}
	}

	return text;
}

time_t Database::timeColumn(sqlite3_stmt * statement, int index) {
	time_t value = sqlite3_column_int64(statement, index);

	if (value == 0 && sqlite3_errcode(conn) == SQLITE_NOMEM) {
		throw runtime_error(sqlite3_errmsg(conn));
	} else {
		return value;
	}
}

void Database::finalize(sqlite3_stmt * statement) {
	if (sqlite3_finalize(statement) != SQLITE_OK)
			throw runtime_error(sqlite3_errmsg(conn));
}

void Database::executeQuery(const string & query) {
	sqlite3_stmt * statement=prepareStatement(query);

	try {
		step(statement);
	} catch (runtime_error &) {
		finalize(statement);
		throw;
	}

	finalize(statement);
}

void Database::upsertHash(const hash & extentsHash, const hash & dataHash) {
	char extents_hash_raw[HASH_LENGTH];
	char data_hash_raw[HASH_LENGTH];

	extentsHash.getBinHash(extents_hash_raw);
	dataHash.getBinHash(data_hash_raw);

	bind(upsertHashStmt, 1, extents_hash_raw);
	bind(upsertHashStmt, 2, data_hash_raw);

	step(upsertHashStmt);
}

void Database::insertFile(const File * file) {
	char extents_hash_raw[HASH_LENGTH];

	file->extentsHash.getBinHash(extents_hash_raw);

	bind(insertFileStmt, 1, * file->filename);
	bind(insertFileStmt, 2, file->mTime);
	bind(insertFileStmt, 3, extents_hash_raw);

	step(insertFileStmt);
}

void Database::updateFile(const File * file) {
	char extents_hash_raw[HASH_LENGTH];

	file->extentsHash.getBinHash(extents_hash_raw);

	bind(updateFileStmt, 1, file->mTime);
	bind(updateFileStmt, 2, extents_hash_raw);
	bind(updateFileStmt, 3, * file->filename);

	step(updateFileStmt);
}

void Database::removeHash(const hash & extentsHash) {
	char extents_hash_raw[HASH_LENGTH];

	extentsHash.getBinHash(extents_hash_raw);

	bind(removeHashStmt, 1, extents_hash_raw);

	step(removeHashStmt);
}

void Database::removeFile(const string & filename) {
	bind(removeFileStmt, 1, filename);

	step(removeFileStmt);
}

void Database::updateFiles(map <string, File> & files,
		list<string> & ignored,
		const set<string> & patterns) {

	for (const string & pattern : patterns) {

		bind(getFilesStmt, 1, pattern);

		while (step(getFilesStmt)) {
			char hash_raw [HASH_LENGTH];
			string filename = stringColumn(getFilesStmt, 0);
			time_t m_time = timeColumn(getFilesStmt, 1);
			blobColumn(getFilesStmt, 2, hash_raw);
			hash data_hash(hash_raw);
			blobColumn(getFilesStmt, 3, hash_raw);
			hash extents_hash(hash_raw);

			auto it = files.find(filename);

			if (it != files.end()) {
				it->second.update(m_time, extents_hash, data_hash);
			} else {
				ignored.emplace_back(filename);
			}
		}

	}
}

void Database::cleanHashes() {
	executeQuery("DELETE FROM hashes WHERE extents_hash IN "
			"(SELECT extents_hash FROM hashes NATURAL LEFT JOIN "
			"files WHERE filename IS NULL GROUP BY extents_hash)");
}

void Database::beginTransaction() {
	executeQuery("BEGIN TRANSACTION");
}

void Database::endTransaction() {
	executeQuery("END TRANSACTION");
}
