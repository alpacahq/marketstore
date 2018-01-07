#include <stdint.h>

void wordCopyInt16(char *src, int offset, int reclen, int nrecs, int16_t *dst);
void wordCopyInt32(char *src, int offset, int reclen, int nrecs, int32_t *dst);
void wordCopyInt64(char *src, int offset, int reclen, int nrecs, int64_t *dst);
void wordCopyFloat32(char *src, int offset, int reclen, int nrecs, float *dst);
void wordCopyFloat64(char *src, int offset, int reclen, int nrecs, double *dst);
