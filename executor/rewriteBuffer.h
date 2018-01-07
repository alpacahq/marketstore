#include <stdio.h>
#include <errno.h>
#include <stdint.h>

typedef struct {
  int64_t epoch;
  int32_t nanos;
} epochTimeStruct;

void rewriteBuffer(char *buffer, int VarRecLen, int numVarRecords, char *newBuffer,
		      int64_t intervals, int64_t intervalStartEpoch);

void getTimeFromTicks(uint32_t intervalStart, int64_t intervalsPerDay,
		         uint32_t intervalTicks, epochTimeStruct *ept);
