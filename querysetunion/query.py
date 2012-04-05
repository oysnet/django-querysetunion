from django.db.models.query import QuerySet
from copy import deepcopy
from django.db import connection

REPR_OUTPUT_SIZE = 20
CHUNK_SIZE = 100
ITER_CHUNK_SIZE = CHUNK_SIZE

class QuerySetUnion(object):

    def __init__(self, query_set=None, ordering = None, low_mark = None, high_mark=None):
        self.query_set = query_set
        self.ordering = ordering
        self._limit_low_mark = low_mark
        self._limit_high_mark = high_mark
        self._iter = None
        self._result_cache = None
        self._iter = None
        self._sticky_filter = False
        self._for_write = False
        self._prefetch_related_lookups = []
        self._prefetch_done = False
        
    def __deepcopy__(self, memo):
        """
        Deep copy of a QuerySet doesn't populate the cache
        """
        obj = self.__class__()
        for k,v in self.__dict__.items():
            if k in ('_iter','_result_cache'):
                obj.__dict__[k] = None
            else:
                obj.__dict__[k] = copy.deepcopy(v, memo)
        return obj
    
    def __getstate__(self):
        """
        Allows the QuerySet to be pickled.
        """
        # Force the cache to be fully populated.
        len(self)

        obj_dict = self.__dict__.copy()
        obj_dict['_iter'] = None
        return obj_dict
    
    def __repr__(self):
        data = list(self[:REPR_OUTPUT_SIZE + 1])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)
    
    def __len__(self):
        # Since __len__ is called quite frequently (for example, as part of
        # list(qs), we make some effort here to be as efficient as possible
        # whilst not messing up any existing iterators against the QuerySet.
        if self._result_cache is None:
            if self._iter:
                self._result_cache = list(self._iter)
            else:
                self._result_cache = list(self.iterator())
        elif self._iter:
            self._result_cache.extend(self._iter)
        if self._prefetch_related_lookups and not self._prefetch_done:
            self._prefetch_related_objects()
        return len(self._result_cache)
    
    def __iter__(self):
        if self._prefetch_related_lookups and not self._prefetch_done:
            # We need all the results in order to be able to do the prefetch
            # in one go. To minimize code duplication, we use the __len__
            # code path which also forces this, and also does the prefetch
            len(self)

        if self._result_cache is None:
            self._iter = self.iterator()
            self._result_cache = []
        if self._iter:
            return self._result_iter()
        # Python's list iterator is better than our version when we're just
        # iterating over the cache.
        return iter(self._result_cache)
    
    def _result_iter(self):
        pos = 0
        while 1:
            upper = len(self._result_cache)
            while pos < upper:
                yield self._result_cache[pos]
                pos = pos + 1
            if not self._iter:
                raise StopIteration
            if len(self._result_cache) <= pos:
                self._fill_cache()
                    
    def __nonzero__(self):
        if self._prefetch_related_lookups and not self._prefetch_done:
            # We need all the results in order to be able to do the prefetch
            # in one go. To minimize code duplication, we use the __len__
            # code path which also forces this, and also does the prefetch
            len(self)

        if self._result_cache is not None:
            return bool(self._result_cache)
        try:
            iter(self).next()
        except StopIteration:
            return False
        return True
    
    def __contains__(self, val):
        # The 'in' operator works without this method, due to __iter__. This
        # implementation exists only to shortcut the creation of Model
        # instances, by bailing out early if we find a matching element.
        pos = 0
        if self._result_cache is not None:
            if val in self._result_cache:
                return True
            elif self._iter is None:
                # iterator is exhausted, so we have our answer
                return False
            # remember not to check these again:
            pos = len(self._result_cache)
        else:
            # We need to start filling the result cache out. The following
            # ensures that self._iter is not None and self._result_cache is not
            # None
            it = iter(self)

        # Carry on, one result at a time.
        while True:
            if len(self._result_cache) <= pos:
                self._fill_cache(num=1)
            if self._iter is None:
                # we ran out of items
                return False
            if self._result_cache[pos] == val:
                return True
            pos += 1
    
    def __getitem__(self,k):
        if not isinstance(k, (slice, int, long)):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0))
                or (isinstance(k, slice) and (k.start is None or k.start >= 0)
                    and (k.stop is None or k.stop >= 0))), \
                "Negative indexing is not supported."
                
        if self._result_cache is not None:
            if self._iter is not None:
                # The result cache has only been partially populated, so we may
                # need to fill it out a bit more.
                if isinstance(k, slice):
                    if k.stop is not None:
                        # Some people insist on passing in strings here.
                        bound = int(k.stop)
                    else:
                        bound = None
                else:
                    bound = k + 1
                if len(self._result_cache) < bound:
                    self._fill_cache(bound - len(self._result_cache))
            return self._result_cache[k]
        
        if isinstance(k, slice):
            clone = self._clone()
            if k.start is not None:
                start = int(k.start)
            else:
                start = None
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = None
            clone.set_limits(start, stop)
            return clone
        
        clone = self._clone()
        clone.set_limits(k, k + 1)
        return list(clone)[0]

    def __and__(self, other):
        raise NotImplementedError

    def __or__(self, other):
        raise NotImplementedError
    

    def models(self,*args):
        if self.query_set:
            raise Exception('Models already defined')
        self.query_set = {}
        for m in args:
            self.query_set[m.__name__] = QuerySet(m)
        return self
    
    def clone_query_set(self):
        qs = {}
        for k, q in self.query_set.iteritems():
            qs[k]=q._clone()
        return qs
    
    def set_limits(self, low=None, high=None):
        self._limit_low_mark = low
        self._limit_high_mark = high
        
    ####################################
    # METHODS THAT DO DATABASE QUERIES #
    ####################################
    def _prepare_queries(self, ordering=True, fields = None):
        
        clone = self._clone()
        
        attrs = {}
        queries   = []
        params = []
        
        high = None
        if self._limit_low_mark and self._limit_high_mark:
            high = self._limit_high_mark
        elif self._limit_low_mark:
            high = None
        elif self._limit_high_mark:
            high = self._limit_high_mark
        
        if fields is not None:
            for k, query_set in clone.query_set.iteritems():
                select = []
                select.extend(fields)
                query_set.query.select = select
                if not clone.ordering or ordering == False:
                    query_set.query.clear_ordering(True)
                if high:
                    query_set.query.set_limits(0,high)
                
                
                compiler = query_set.query.get_compiler('default')
                tmp_sql, tmp_params = compiler.as_sql()
                queries.append(tmp_sql)
                params.extend(tmp_params)
                
        else:    
        
            all_attrs = set()
            
            for k, query_set in clone.query_set.iteritems():
                compiler = query_set.query.get_compiler('default')
                compiler.pre_sql_setup()
                aliases = set(compiler.query.extra_select.keys())
                col_aliases = aliases.copy()
                cols, new_aliases = compiler.get_default_columns(True,
                        col_aliases)
                
                attrs[k] = [list(new_aliases),[ v.split('.')[1] for v in new_aliases],{}]
                all_attrs.update([ v.split('.')[1] for v in new_aliases])
            
            for k, query_set in clone.query_set.iteritems():
                select = []
                select.append(Attribute("'%s'" % k,'_model'))
                i = 1      
                for attr in all_attrs:
                    if attr in attrs[k][1]:
                        select.append(Attribute(attrs[k][0][attrs[k][1].index(attr)],attr))
                        attrs[k][2][attr]=i
                    else:
                        select.append(Attribute('null',attr))
                    i+=1
                query_set.query.select = select
                if not clone.ordering or ordering == False:
                    query_set.query.clear_ordering(True)
                if high:
                    query_set.query.set_limits(0,high)
                
                
                compiler = query_set.query.get_compiler('default')
                tmp_sql, tmp_params = compiler.as_sql()
                queries.append(tmp_sql)
                params.extend(tmp_params)
                
        return queries, params, attrs
    
    def iterator(self):
        """
        An iterator over the results from applying this QuerySet to the
        database.
        """
        
        
        clone = self._clone()
        
        queries, params, attrs = self._prepare_queries()    
        sql = 'SELECT * FROM (%s) as a' %' UNION '.join(['(%s)' % r for r in queries])
        if clone.ordering:
            sql+=(' ORDER BY %s' % ', '.join(['%s %s' % (field.strip('-'),'ASC' if field[0] != '-' else 'DESC') for field in clone.ordering]))
            
            

        
        if clone._limit_high_mark is not None:
            l = 0 if clone._limit_low_mark is None else clone._limit_low_mark
            sql += ' LIMIT %d' % (clone._limit_high_mark - l)
            #print sql
        if clone._limit_low_mark is not None:
            sql += ' OFFSET %d' % clone._limit_low_mark            
        
        
        #print sql
        
        cursor = connection.cursor()
        cursor.execute(sql, params)
        for r in cursor.fetchall():
            ct = r[0]
            model = self.query_set[ct].model
            kwargs = {}
            for attr,pos in attrs[ct][2].iteritems():
                kwargs[attr.strip('`')]=r[pos]
            yield model(**kwargs)
        
            
    def aggregate(self, *args, **kwargs):
        raise NotImplementedError
    
    def count(self):
        clone = self._clone()
        
        queries, params, attrs = clone._prepare_queries(ordering=False, fields=[Attribute('count(*)','cnt')])    
        sql = 'SELECT sum(cnt) FROM (%s) as a' %' UNION '.join(['(%s)' % r for r in queries])
        cursor = connection.cursor()
        cursor.execute(sql, params)
        c = cursor.fetchone()[0]
        print sql
        print "===",c
        return int(c)    
    
    def get(self, *args, **kwargs):
        raise NotImplementedError("Have one's cake and eat it too")

    def get_or_create(self, **kwargs):
        raise NotImplementedError("Have one's cake and eat it too")
    
    def create(self, **kwargs):
        raise NotImplementedError("Have one's cake and eat it too")

    def bulk_create(self, objs):
        raise NotImplementedError("Have one's cake and eat it too")

    def latest(self, field_name=None):
        raise NotImplementedError
    
    def in_bulk(self, id_list):
        raise NotImplementedError("Have one's cake and eat it too")
    
    def delete(self):
        raise NotImplementedError("Have one's cake and eat it too")
    
    def update(self):
        raise NotImplementedError("Have one's cake and eat it too")
    
    def exists(self):
        raise NotImplementedError
    
    ##################################################
    # PUBLIC METHODS THAT RETURN A QUERYSET SUBCLASS #
    ##################################################

    def values(self, *fields):
        raise NotImplementedError

    def values_list(self, *fields, **kwargs):
        raise NotImplementedError
    
    def dates(self, field_name, kind, order='ASC'):
        raise NotImplementedError
    
    def none(self):
        raise NotImplementedError
    
    ##################################################################
    # PUBLIC METHODS THAT ALTER ATTRIBUTES AND RETURN A NEW QUERYSET #
    ##################################################################

    def all(self):
        """
        Returns a new QuerySet that is a copy of the current one. This allows a
        QuerySet to proxy for a model manager in some cases.
        """
        return self._clone()
    
    def filter(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with the args ANDed to the existing
        set.
        """
        return self._filter_or_exclude(False, *args, **kwargs)

    def exclude(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with NOT (args) ANDed to the existing
        set.
        """
        return self._filter_or_exclude(True, *args, **kwargs)


    def _filter_or_exclude(self, negate, *args, **kwargs):
        """
        if args or kwargs:
            assert self.query.can_filter(), \
                    "Cannot filter a query once a slice has been taken."
        """
        clone = self._clone()
        for k, q in clone.query_set.iteritems():
            clone.query_set[k] = q._filter_or_exclude( negate, *args, **kwargs)
        return clone
    
    def complex_filter(self, filter_obj):
        raise NotImplementedError
    
    def select_for_update(self, **kwargs):
        raise NotImplementedError("Have one's cake and eat it too")
    
    def select_related(self, *fields, **kwargs):
        raise NotImplementedError
    
    def prefetch_related(self, *lookups):
        raise NotImplementedError
    
    def dup_select_related(self, other):
        raise NotImplementedError
    
    def annotate(self, *args, **kwargs):
        raise NotImplementedError
    
    def order_by(self, *field_names):
        clone = self._clone()
        for k, q in clone.query_set.iteritems():
            clone.query_set[k] = q.order_by(*field_names)
        clone.ordering = field_names
        return clone
    
        return clone
    
    def distinct(self, *field_names):
        raise NotImplementedError
    
    def extra(self, select=None, where=None, params=None, tables=None,
              order_by=None, select_params=None):
        raise NotImplementedError
    
    def reverse(self):
        raise NotImplementedError
    
    def defer(self, *fields):
        raise NotImplementedError
    
    def only(self, *fields):
        raise NotImplementedError
    
    def using(self, alias):
        raise NotImplementedError      
    
    ###################################
    # PUBLIC INTROSPECTION ATTRIBUTES #
    ###################################

    def ordered(self):
        return sefl.ordering
    ordered = property(ordered)

    @property
    def db(self):
        raise NotImplementedError

    ###################
    # PRIVATE METHODS #
    ###################
    def _clone(self):
        return Manager(query_set=self.clone_query_set(),ordering = deepcopy(self.ordering),low_mark = self._limit_low_mark,high_mark = self._limit_high_mark)

    def _fill_cache(self, num=None):
        if self._iter:
            try:
                for i in range(num or ITER_CHUNK_SIZE):
                    self._result_cache.append(self._iter.next())
            except StopIteration:
                self._iter = None

    def _next_is_sticky(self):
        raise NotImplementedError

    def _merge_sanity_check(self, other):
        raise NotImplementedError

    def _setup_aggregate_query(self, aggregates):
        raise NotImplementedError

    def _prepare(self):
        return self

    def _as_sql(self, connection):
        raise NotImplementedError

    # When used as part of a nested query, a queryset will never be an "always
    # empty" result.
    value_annotation = True
                   
class Attribute():
    def __init__(self,col,alias):
        self.alias = alias
        self.col = col
    def as_sql(self,*args,**kwargs):
        return '%s AS %s' % (self.col, self.alias)
       


        