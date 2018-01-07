#include "rewriteBuffer.h"

#include <stdio.h>

double ticksPerIntervalDivSecsPerDay = ((double)49710.269629629629629629629629629);

void rewriteBuffer(char *buffer, int VarRecLen, int numVarRecords, char *newBuffer,
		      int64_t intervals, int64_t intervalStartEpoch) {
	int nbCursor=0;
	uint32_t *ticks;
	epochTimeStruct ept;
	for (int j = 0; j < numVarRecords; j++) {
             ticks = (uint32_t *)(buffer+(VarRecLen-4));
	 	// Expand ticks (32-bit) into epoch and nanos (96-bits)
	 	getTimeFromTicks(intervalStartEpoch, intervals, *ticks, &ept);
		//printf("Epoch2 = %ld\n",epoch);
	 	for (int ii = 0; ii < 8; ii++) {
	 		newBuffer[nbCursor+ii] = *((char *)(&(ept.epoch)) + ii);
	 	}
	 	nbCursor += 8;
	 	for (int ii = 0; ii < VarRecLen-4; ii++) {
	 		newBuffer[nbCursor+ii] = buffer[ii];
	 	}
	 	nbCursor += VarRecLen - 4;
	 	for (int ii = 0; ii < 4; ii++) {
	 		newBuffer[nbCursor+ii] = *((char *)(&(ept.nanos)) + ii);
	 	}
	 	nbCursor += 4;
             buffer += VarRecLen;
	}
}
inline void getTimeFromTicks(uint32_t intervalStart, int64_t intervalsPerDay,
		         uint32_t intervalTicks, epochTimeStruct *ept) { //Return values
/*
   Takes two time components, the start of the interval and the number of
   interval ticks to the timestamp and returns an epoch time (seconds) and
   the number of nanoseconds of fractional time within the last second as
   a remainder
*/
	int64_t intStart=intervalStart;
	double intTicks=intervalTicks;
	double fractionalSeconds = intTicks / (intervalsPerDay * ticksPerIntervalDivSecsPerDay);
	double subseconds = 1000000000 * (fractionalSeconds - (int64_t)fractionalSeconds);
	//printf("FracSecs: %f, SubSecs: %f\n",fractionalSeconds, subseconds);
	if (subseconds >= 1000000000) {
		subseconds -= 1000000000;
		fractionalSeconds += 1;
	}
	ept->epoch = intStart + fractionalSeconds;
	ept->nanos = subseconds;
}
