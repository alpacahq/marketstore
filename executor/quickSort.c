#include "quickSort.h"

void _quick_sort_uint32_end(char *a,int l,int u,int reclen, char *temp);
int partition_uint32_end(char *a,int l,int u,int reclen,char * temp);

void quickSortKeyAtEndUINT32(char *a, int64_t len, int64_t reclen) {
	struct testStr *tt;
	// Allocate a double temp record
	char *temp = (char *)malloc(sizeof(char)*2*reclen);
	int nrecords = len / reclen;
	_quick_sort_uint32_end(a,0,nrecords-1,reclen, temp);
	free(temp);
}

void _quick_sort_uint32_end(char *a,int l,int u,int reclen, char *temp) {
	int j;
	if ( l < u ) {
		j=partition_uint32_end(a,l,u,reclen,temp);
		_quick_sort_uint32_end(a,l,j-1,reclen,temp);
		_quick_sort_uint32_end(a,j+1,u,reclen,temp);
	}
}

int partition_uint32_end(char *a,int l,int u,int reclen,char * temp) {
	int i,j;
	char *v=temp, *slot=temp+reclen;

	memcpy(v,a+l*reclen,reclen);
	i=l;
	j=u+1;

	do {
		do
			i++;
		while( *(uint32_t *)(a+i*reclen+(reclen-4)) < *(uint32_t *)(v+reclen-4) && i <= u);

		do
			j--;
		while( *(uint32_t *)(v+reclen-4) < *(uint32_t *)(a+j*reclen+(reclen-4)) );

		if( i < j )
		{
			memcpy(slot,a+i*reclen,reclen);
			memcpy(a+i*reclen,a+j*reclen,reclen);
			memcpy(a+j*reclen,slot,reclen);
		}
	} while( i < j );

	memcpy(a+l*reclen,a+j*reclen,reclen);
	memcpy(a+j*reclen,v,reclen);

	return(j);
}
