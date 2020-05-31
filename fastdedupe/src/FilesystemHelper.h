/*
 * Fiemap.h
 *
 *  Created on: May 23, 2020
 *      Author: adam
 */

#ifndef FILESYSTEMHELPER_H_
#define FILESYSTEMHELPER_H_

#include <sys/types.h>
#include <dirent.h>

#include <ctime>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

struct FiemapData {
		const size_t logical;
		const size_t physical;
		const size_t length;
};

struct FileInfo {
	const std::string filename;
	const long int size;
	const std::time_t mTime;
	const bool dirType;
	const bool regType;
};

struct IgnoredFile {
	const std::string fullpath;
	const int error;
	const std::string message;
};

class FilesystemHelper {
private:
	static FilesystemHelper filesystemHelper;
	char * buffer;
	long int minSize;

	FilesystemHelper(long int minSize);

	void getStat(const std::string & filename, struct stat & statData);

	struct fiemap * getFiemap(int fd, size_t offset, size_t size);

	int dedupe(int src_fd, size_t size,
			const std::vector<std::string> &dsts_vector, int error,
			std::map<std::string, std::string> &failures);

	void recursiveRead(const std::string & fullpath,
			DIR * dirStream,
			std::list<FileInfo> & files,
			std::list<IgnoredFile> & ignored);

public:
	static FilesystemHelper & getFilesystemHelper();

	virtual ~FilesystemHelper();

	long int getMinSize();

	void setMinSize(long int minSize);

	std::string getRealPath(const std::string & path);

	std::vector<FiemapData> getFiemapData(
			std::string const & filename, size_t size);

	void dedupe(const std::string & dest,
			const std::set<std::string> & srcs,
			size_t size,
			std::map<std::string, std::string> & failures);

	void fetchInfos(const std::set<std::string> & filenames,
			std::list<FileInfo> & files,
			std::set<std::string> & patterns,
			std::list<IgnoredFile> & ignored);

	void recursiveRead(const std::set<std::string> & filenames,
			std::list<FileInfo> & files,
			std::set<std::string> & patterns,
			std::list<IgnoredFile> & ignored);
};

#endif /* FILESYSTEMHELPER_H_ */
