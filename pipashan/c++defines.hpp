#ifndef PIPASHAN_CXX_DEFINES_INCLUDED
#define PIPASHAN_CXX_DEFINES_INCLUDED

#ifdef __APPLE__
#	define PIPASHAN_OS_APPLE
#elif (defined(linux) || defined(__linux) || defined(__linux__) || defined(__GNU__) || defined(__GLIBC__)) && !defined(_CRAYC)	//Linux
#	define PIPASHAN_OS_LINUX
#elif defined(_WIN32) || defined(__WIN32__) || defined(WIN32)	//Microsoft Windows
#	define PIPASHAN_OS_WINDOWS
#endif

#endif