/*-
 * The MIT License
 *
 * Copyright (c) 2011 Seoul National University.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * Contact: Hyeshik Chang <hyeshik@snu.ac.kr>
 */

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "tabix.h"

static PyObject *TabixError;

typedef struct {
    PyObject_HEAD
    tabix_t *tb;
    char *fn;
} TabixObject;

typedef struct {
    PyObject_HEAD
    TabixObject *tbobj;
    ti_iter_t iter;
} TabixIteratorObject;

static PyTypeObject Tabix_Type, TabixIterator_Type;

/* --- TabixIterator --------------------------------------------------- */

static PyObject *
tabixiter_create(TabixObject *parentidx, ti_iter_t iter)
{
    TabixIteratorObject *self;

    self = PyObject_New(TabixIteratorObject, &TabixIterator_Type);
    if (self == NULL)
        return NULL;

    Py_INCREF(parentidx);
    self->tbobj = parentidx;
    self->iter = iter;

    return (PyObject *)self;
}

static void
tabixiter_dealloc(TabixIteratorObject *self)
{
    ti_iter_destroy(self->iter);
    Py_DECREF(self->tbobj);
    PyObject_Del(self);
}

static PyObject *
tabixiter_iter(PyObject *self)
{
    Py_INCREF(self);
    return self;
}

#if PY_MAJOR_VERSION < 3
# define PYOBJECT_FROM_STRING_AND_SIZE PyString_FromStringAndSize
#else
# define PYOBJECT_FROM_STRING_AND_SIZE PyUnicode_FromStringAndSize
#endif

static PyObject *
tabixiter_iternext(TabixIteratorObject *self)
{
    const char *chunk;
    int len, i;

    chunk = ti_read(self->tbobj->tb, self->iter, &len);
    if (chunk != NULL) {
        PyObject *ret, *column;
        Py_ssize_t colidx;
        const char *ptr, *begin;

        ret = PyList_New(0);
        if (ret == NULL)
            return NULL;

        colidx = 0;
        ptr = begin = chunk;
        for (i = len; i > 0; i--, ptr++)
            if (*ptr == '\t') {
                column = PYOBJECT_FROM_STRING_AND_SIZE(begin,
                                                       (Py_ssize_t)(ptr - begin));
                if (column == NULL || PyList_Append(ret, column) == -1) {
                    Py_DECREF(ret);
                    return NULL;
                }

                Py_DECREF(column);
                begin = ptr + 1;
                colidx++;
            }

        column = PYOBJECT_FROM_STRING_AND_SIZE(begin, (Py_ssize_t)(ptr - begin));
        if (column == NULL || PyList_Append(ret, column) == -1) {
            Py_DECREF(ret);
            return NULL;
        }
        Py_DECREF(column);

        return ret;
    }
    else
        return NULL;
}

static PyMethodDef tabixiter_methods[] = {
    {NULL, NULL} /* sentinel */
};

static PyTypeObject TabixIterator_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "tabix.TabixIterator",      /*tp_name*/
    sizeof(TabixIteratorObject), /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)tabixiter_dealloc,  /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    tabixiter_iter,             /*tp_iter*/
    (iternextfunc)tabixiter_iternext, /*tp_iternext*/
    tabixiter_methods,          /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};


/* --- Tabix ----------------------------------------------------------- */

static PyObject *
tabix_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    TabixObject *self;
    const char *fn, *fnidx=NULL;
    static char *kwnames[]={"fn", "fnidx", NULL};
    tabix_t *tb;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|z:Tabix",
                                     kwnames, &fn, &fnidx))
        return NULL;

    tb = ti_open(fn, fnidx);
    if (tb == NULL) {
        PyErr_SetString(TabixError, "Can't open the index file.");
        return NULL;
    }

    self = (TabixObject *)type->tp_alloc(type, 0);
    if (self == NULL)
        return NULL;

    self->tb = tb;
    self->fn = strdup(fn);

    return (PyObject *)self;
}

static void
tabix_dealloc(TabixObject *self)
{
    free(self->fn);
    ti_close(self->tb);
    PyObject_Del(self);
}

static PyObject *
tabix_query(TabixObject *self, PyObject *args)
{
    char *name;
    int begin, end;
    ti_iter_t result;

    if (!PyArg_ParseTuple(args, "sii:query", &name, &begin, &end))
        return NULL;

    result = ti_query(self->tb, name, begin, end);
    if (result == NULL) {
        PyErr_SetString(TabixError, "query failed");
        return NULL;
    }

    return tabixiter_create(self, result);
}

static PyObject *
tabix_queryi(TabixObject *self, PyObject *args)
{
    int tid, begin, end;
    ti_iter_t result;

    if (!PyArg_ParseTuple(args, "iii:queryi", &tid, &begin, &end))
        return NULL;

    result = ti_queryi(self->tb, tid, begin, end);
    if (result == NULL) {
        PyErr_SetString(TabixError, "query failed");
        return NULL;
    }

    return tabixiter_create(self, result);
}

static PyObject *
tabix_querys(TabixObject *self, PyObject *args)
{
    const char *reg;
    ti_iter_t result;

    if (!PyArg_ParseTuple(args, "s:querys", &reg))
        return NULL;

    result = ti_querys(self->tb, reg);
    if (result == NULL) {
        PyErr_SetString(TabixError, "query failed");
        return NULL;
    }

    return tabixiter_create(self, result);
}

static PyObject *
tabix_repr(TabixObject *self)
{
#if PY_MAJOR_VERSION < 3
    return PyString_FromFormat("<Tabix fn=\"%s\">", self->fn);
#else
    return PyUnicode_FromFormat("<Tabix fn=\"%s\">", self->fn);
#endif
}

static PyMethodDef tabix_methods[] = {
    {"query",           (PyCFunction)tabix_query, METH_VARARGS,
        PyDoc_STR("T.query(name, begin, end) -> iterator")},
    {"queryi",          (PyCFunction)tabix_queryi, METH_VARARGS,
        PyDoc_STR("T.queryi(tid, begin, id) -> iterator")},
    {"querys",          (PyCFunction)tabix_querys, METH_VARARGS,
        PyDoc_STR("T.querys(region) -> iterator")},
    {NULL,              NULL}           /* sentinel */
};

static PyTypeObject Tabix_Type = {
    /* The ob_type field must be initialized in the module init function
     * to be portable to Windows without using C++. */
    PyVarObject_HEAD_INIT(NULL, 0)
    "tabix.Tabix",              /*tp_name*/
    sizeof(TabixObject),        /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    /* methods */
    (destructor)tabix_dealloc,  /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    (reprfunc)tabix_repr,       /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    tabix_methods,              /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    (newfunc)tabix_new,         /*tp_new*/
    0,                          /*tp_free*/
    0,                          /*tp_is_gc*/
};
/* --------------------------------------------------------------------- */

static PyMethodDef tabix_functions[] = {
    {NULL, NULL} /* sentinel */
};

PyDoc_STRVAR(module_doc,
"Python interface to tabix, Heng Li's generic indexer for TAB-delimited "
"genome position filesThis is a template module just for instruction.");

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef tabixmodule = { 
    PyModuleDef_HEAD_INIT,
    "tabix",
    module_doc,
    -1, 
    tabix_functions,
    NULL,
    NULL,
    NULL,
    NULL
};
#endif

#if PY_MAJOR_VERSION < 3
PyMODINIT_FUNC inittabix(void)
#else
PyMODINIT_FUNC PyInit_tabix(void)
#endif
{
    PyObject *m;

    if (PyType_Ready(&Tabix_Type) < 0)
        goto fail;
    if (PyType_Ready(&TabixIterator_Type) < 0)
        goto fail;

#if PY_MAJOR_VERSION < 3
    m = Py_InitModule3("tabix", tabix_functions, module_doc);
#else
    m = PyModule_Create(&tabixmodule);
#endif
    if (m == NULL)
        goto fail;

    if (TabixError == NULL) {
        TabixError = PyErr_NewException("tabix.error", NULL, NULL);
        if (TabixError == NULL)
            goto fail;
    }
    Py_INCREF(TabixError);
    PyModule_AddObject(m, "error", TabixError);

    PyModule_AddObject(m, "Tabix", (PyObject *)&Tabix_Type);
    PyModule_AddObject(m, "TabixIterator", (PyObject *)&TabixIterator_Type);

#if PY_MAJOR_VERSION >= 3
    return m;
#endif

 fail:
#if PY_MAJOR_VERSION < 3
    return;
#else
    return NULL;
#endif
}
