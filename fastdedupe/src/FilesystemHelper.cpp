/*
 * Fiemap.cpp
 *
 *  Created on: May 23, 2020
 *      Author: adam
 */

#include "FilesystemHelper.h"

#include <linux/fs.h>
#include <linux/fiemap.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>

#include <list>
#include <set>
#include <stdexcept>
#include <vector>

using std::invalid_argument;
using std::list;
using std::logic_error;
using std::map;
using std::runtime_error;
using std::set;
using std::string;
using std::vector;

const size_t PAGE_SIZE = getpagesize();
const size_t FILESYSTEM_BUFFER_SIZE = PAGE_SIZE > PATH_MAX ?
		PAGE_SIZE : PATH_MAX;
const size_t FIEMAP_MAX_EXTENTS = (FILESYSTEM_BUFFER_SIZE
		-sizeof(struct fiemap))/sizeof(struct fiemap_extent);
const size_t DEDUPE_MAX_DSTS = (PAGE_SIZE -
		sizeof(struct file_dedupe_range))
				/ sizeof(file_dedupe_range_info);
const size_t DEDUPE_MAX_SIZE = 16*1024*1024;
const long int RECURSIVE_DEFAULT_MIN_SIZE = 128*1024;


FilesystemHelper FilesystemHelper::filesystemHelper(RECURSIVE_DEFAULT_MIN_SIZE);

FilesystemHelper::FilesystemHelper(long int minSize) :
	buffer(new char [FILESYSTEM_BUFFER_SIZE]) {
	if (buffer == NULL) {
		throw runtime_error("Unable to allocate buffer");
	}
	this->minSize = minSize;
}

FilesystemHelper::~FilesystemHelper() {
	delete [] buffer;
}

long int FilesystemHelper::getMinSize() {
	return minSize;
}

void FilesystemHelper::setMinSize(long int minSize) {
	this->minSize = minSize;
}

FilesystemHelper & FilesystemHelper::getFilesystemHelper() {
	return filesystemHelper;
}

void FilesystemHelper::getStat(const string & filename, struct stat & statData) {
	if ( lstat(filename.c_str(), &statData) != 0 ) {
		throw errno;
	}
}

struct fiemap * FilesystemHelper::getFiemap(int fd, size_t offset, size_t size) {
	struct fiemap *fiemap_buffer = (struct fiemap *) buffer;

	fiemap_buffer->fm_start=offset;
	fiemap_buffer->fm_length=size;
	fiemap_buffer->fm_flags=FIEMAP_FLAG_SYNC;
	fiemap_buffer->fm_extent_count=FIEMAP_MAX_EXTENTS;
	fiemap_buffer->fm_reserved=0;

	if (ioctl(fd, FS_IOC_FIEMAP, fiemap_buffer) == -1) {
		throw runtime_error(strerror(errno));
	}

	return fiemap_buffer;
}

vector<FiemapData> FilesystemHelper::getFiemapData(string const & filename, size_t size) {
	int fd = open(filename.c_str(), O_RDONLY|O_NOATIME|O_NOFOLLOW);
	if (fd == -1) {
		if (errno == ENOENT)
			throw invalid_argument(strerror(errno));
		else
			throw runtime_error(strerror(errno));
	}
	vector<FiemapData> extents;
	struct fiemap * fiemap_data;
	struct fiemap_extent * last;
	size_t offset = 0;
	size_t pending_size = size;
	try {
		do {
			fiemap_data = getFiemap(fd, offset, pending_size);

			for (size_t i=0;
					i<fiemap_data->fm_mapped_extents; i++) {
				struct fiemap_extent & extent =
						fiemap_data->fm_extents[i];

				if (! (extent.fe_flags
						& FIEMAP_EXTENT_DATA_INLINE) ) {

					extents.emplace_back(FiemapData{
						extent.fe_logical, extent.fe_physical,
						extent.fe_length});

					last = &extent;

				}
			}

			if (extents.size() == 0) {
				if (offset==0)
					throw invalid_argument("No mapped extents");
				else
					throw logic_error("Last extent flag not set");
			}

			offset = last->fe_logical + last->fe_length;
			pending_size = size - offset;
		} while (! (last->fe_flags & FIEMAP_EXTENT_LAST) );
	} catch (...) {
		close(fd);
		throw;
	}

	close(fd);

	return extents;
}

int FilesystemHelper::dedupe(int src_fd, size_t size,
		const vector<string> &dsts_vector,
		int error,	map<string, string> &failures) {
	struct file_dedupe_range * range =
			(struct file_dedupe_range *) buffer;
	struct file_dedupe_range_info * range_infos = range->info;

	range->dest_count = dsts_vector.size();

	do {
		range->src_length = size > DEDUPE_MAX_SIZE ?
				DEDUPE_MAX_SIZE : size;
		if (ioctl(src_fd, FIDEDUPERANGE, range) == -1)
			error = errno;

		size -= range->src_length;
		if (size > 0) {
			range->src_offset += range->src_length;
			for (size_t j = 0; j < range->dest_count; j++) {
				int status = range_infos[j].status;
				if (status == FILE_DEDUPE_RANGE_DIFFERS) {
					failures.emplace(dsts_vector[j], "Files differ");
				} else if (status != FILE_DEDUPE_RANGE_SAME) {
					failures.emplace(dsts_vector[j], "Unknown error");
				}

				range_infos[j].dest_offset += range->src_length;
			}
		}
	} while (size > 0);

	return error;
}

void FilesystemHelper::dedupe(const string & src,
		const set<string> & dests, size_t size,
		map<string, string> & failures) {

	struct file_dedupe_range * range =
			(struct file_dedupe_range *) buffer;
	struct file_dedupe_range_info * range_infos =
			range->info;

	int src_fd = open(src.c_str(), O_RDONLY|O_NOATIME|O_NOFOLLOW);
	if (src_fd == -1) throw invalid_argument(strerror(errno));

	int error = 0;

	size_t i = 0;
	range->src_offset = 0;
	range->reserved1 = 0;
	range->reserved2 = 0;
	vector<string> dsts_vector;
	dsts_vector.reserve(DEDUPE_MAX_DSTS);

	for (auto & dest : dests) {

		struct file_dedupe_range_info & range_info =
				range_infos[i];

		range_info.dest_fd = open(dest.c_str(),
				O_RDONLY|O_NOATIME|O_NOFOLLOW);
		if (range_info.dest_fd == -1) {
			failures.emplace(dest, strerror(errno));
			continue;
		}

		dsts_vector.push_back(dest);

		range_info.dest_offset=0;
		range_info.reserved=0;

		i++;

		if (i==DEDUPE_MAX_DSTS) {
			error = dedupe(src_fd, size, dsts_vector, error,
					failures);

			for (size_t j = 0; j<i; j++)
				close(range_infos[j].dest_fd);

			i=0;
			range->src_offset = 0;
			range->reserved1 = 0;
			range->reserved2 = 0;
			dsts_vector.clear();
			dsts_vector.reserve(DEDUPE_MAX_DSTS);
		}
	}

	if (i>0) {
		error = dedupe(src_fd, size, dsts_vector, error,
			failures);

		for (size_t j = 0; j<i; j++)
			close(range_infos[j].dest_fd);
	}

	close(src_fd);

	if (error)
		throw runtime_error(strerror(errno));
}

string FilesystemHelper::getRealPath(const string & path) {
	char * fullpath = realpath(path.c_str(), buffer);

	if (fullpath == NULL) throw errno;
	else return fullpath;
}

void FilesystemHelper::fetchInfos(const set<string> & filenames,
		list <FileInfo> & files,
		set<string> & patterns,
		list<IgnoredFile> & ignored) {
	for (auto & file : filenames) {
		try {
			struct stat statData;

			getStat(file, statData);

			string fullpath(getRealPath(file));
			patterns.emplace(fullpath);

			if (S_ISREG(statData.st_mode)) {
				files.emplace_back(FileInfo{fullpath,
					statData.st_size,
					statData.st_mtim.tv_sec,
					false, true});
			} else if (S_ISDIR(statData.st_mode)) {
				files.emplace_back(FileInfo{fullpath,
					statData.st_size,
					statData.st_mtim.tv_sec,
					true, false});
			}
		} catch (int error) {
			ignored.emplace_back(IgnoredFile{
				file,
				error,
				strerror(error)
			});
		}

	}
}

void FilesystemHelper::recursiveRead(const string & fullpath,
		DIR * dirStream,
		list<FileInfo> & files,
		list<IgnoredFile> & ignored) {
	
	errno = 0;

	for (	struct dirent * entity = readdir(dirStream);
			entity!=NULL;
			errno = 0, entity = readdir(dirStream) ) {
		if (!strcmp(entity->d_name, ".") ||
				!strcmp(entity->d_name, "..")) {
			errno = 0;
			continue;
		}

		string entity_fullpath(fullpath+"/"+entity->d_name);
		bool dir_type = false;
		bool regular_type = false;
		try {
			struct stat statData;

			switch (entity->d_type) {
			case DT_UNKNOWN:
				getStat(entity_fullpath, statData);

				if (S_ISDIR(statData.st_mode)) {
					dir_type = true;
				} else if (S_ISREG(statData.st_mode)) {
					regular_type = true;
				}
				break;
			case DT_DIR:
				dir_type = true;
				break;
			case DT_REG:
				getStat(entity_fullpath, statData);
				regular_type = true;
				break;
			}

			if (dir_type) {
				DIR * entityDirStream = opendir(
						entity_fullpath.c_str());
				if (entityDirStream == NULL)
					ignored.emplace_back(
							IgnoredFile{entity_fullpath,
							errno,
							strerror(errno)});
				else {
					recursiveRead(entity_fullpath,
							entityDirStream,
							files, ignored);
					closedir(entityDirStream);
				}
			} else if (regular_type) {
				if (statData.st_size > minSize)
					files.emplace_back(FileInfo{entity_fullpath,
						statData.st_size,
						statData.st_mtim.tv_sec,
						false, true});
			}

		} catch (int error) {
			ignored.emplace_back(
					IgnoredFile{
						entity_fullpath,
						error,
						strerror(error)});
		}

		errno = 0;
	}
	
	if (errno) {
		ignored.emplace_back(
			IgnoredFile{
				fullpath,
				errno,
				strerror(errno)});
	}

	
}

void FilesystemHelper::recursiveRead(const set<string> & filenames,
		list <FileInfo> & files,
		set <string> & patterns,
		list<IgnoredFile> & ignored) {

	fetchInfos(filenames, files, patterns, ignored);

	for (const auto & fileinfo : files) {

		if (! fileinfo.dirType)
			continue;

		const string & dir = fileinfo.filename;

		try {
			DIR * dirStream = opendir(dir.c_str());
			if (dirStream == NULL)
				ignored.emplace_back(IgnoredFile{
					dir,
					errno,
					strerror(errno)
				});
			else {
				recursiveRead(dir,
						dirStream,
						files, ignored);
				closedir(dirStream);
			}
		} catch (int error) {
			ignored.emplace_back(IgnoredFile{
				dir,
				error,
				strerror(error)
			});
		}
	}
}
