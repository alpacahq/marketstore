#include "utilityfuncs.h"

void wordCopyInt16(char *src, int offset, int reclen, int nrecs, int16_t *dst) {
	for (int i=0; i<nrecs; i++) {
		dst[i] = *((int16_t *)(src+(i*reclen+offset)));
	}
}

void wordCopyInt32(char *src, int offset, int reclen, int nrecs, int32_t *dst) {
	for (int i=0; i<nrecs; i++) {
		dst[i] = *((int32_t *)(src+(i*reclen+offset)));
	}
}

void wordCopyInt64(char *src, int offset, int reclen, int nrecs, int64_t *dst) {
	for (int i=0; i<nrecs; i++) {
		dst[i] = *((int64_t *)(src+(i*reclen+offset)));
	}
}

void wordCopyFloat32(char *src, int offset, int reclen, int nrecs, float *dst) {
	for (int i=0; i<nrecs; i++) {
		dst[i] = *((float *)(src+(i*reclen+offset)));
	}
}

void wordCopyFloat64(char *src, int offset, int reclen, int nrecs, double *dst) {
	for (int i=0; i<nrecs; i++) {
		dst[i] = *((double *)(src+(i*reclen+offset)));
	}
}
