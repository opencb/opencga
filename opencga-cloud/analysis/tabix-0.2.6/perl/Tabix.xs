#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include <stdlib.h>
#include "tabix.h"

MODULE = Tabix PACKAGE = Tabix

tabix_t*
tabix_open(fn, fnidx=0)
	char *fn
	char *fnidx
  CODE:
	RETVAL = ti_open(fn, fnidx);
  OUTPUT:
	RETVAL

void
tabix_close(t)
	tabix_t *t
  CODE:
	ti_close(t);

ti_iter_t
tabix_query(t, seq=0, beg=0, end=0x7fffffff)
	tabix_t *t
	const char *seq
	int beg
	int end
  PREINIT:
  CODE:
	RETVAL = ti_query(t, seq, beg, end);
  OUTPUT:
	RETVAL

SV*
tabix_read(t, iter)
	tabix_t *t
	ti_iter_t iter
  PREINIT:
	const char *s;
	int len;
  CODE:
	s = ti_read(t, iter, &len);
	if (s == 0)
	   return XSRETURN_EMPTY;
	RETVAL = newSVpv(s, len);
  OUTPUT:
	RETVAL

void
tabix_getnames(t)
	tabix_t *t
  PREINIT:
	const char **names;
	int i, n;
  PPCODE:
	ti_lazy_index_load(t);
	names = ti_seqname(t->idx, &n);
	for (i = 0; i < n; ++i)
		XPUSHs(sv_2mortal(newSVpv(names[i], 0)));
	free(names);

MODULE = Tabix PACKAGE = TabixIterator

void
tabix_iter_free(iter)
	ti_iter_t iter
  CODE:
	ti_iter_destroy(iter);
