#include "utilityfuncs.h"

void wordCopyInt8(char *src, int offset, int reclen, int nrecs, int8_t *dst) {
	for (int i = 0; i < nrecs; i++){
		dst[i] = *((int8_t *)(src + (i * reclen + offset)));
	}
}

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

void wordCopyUInt8(char *src, int offset, int reclen, int nrecs, uint8_t *dst){
	for (int i = 0; i < nrecs; i++){
		dst[i] = *((uint8_t *)(src + (i * reclen + offset)));
	}
}

void wordCopyUInt16(char *src, int offset, int reclen, int nrecs, uint16_t *dst){
	for (int i = 0; i < nrecs; i++){
		dst[i] = *((uint16_t *)(src + (i * reclen + offset)));
	}
}

void wordCopyUInt32(char *src, int offset, int reclen, int nrecs, uint32_t *dst){
	for (int i = 0; i < nrecs; i++){
		dst[i] = *((uint32_t *)(src + (i * reclen + offset)));
	}
}

void wordCopyUInt64(char *src, int offset, int reclen, int nrecs, uint64_t *dst){
	for (int i = 0; i < nrecs; i++){
		dst[i] = *((uint64_t *)(src + (i * reclen + offset)));
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
