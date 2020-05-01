from collections import namedtuple
import datetime
import pandas as pd
from django.db.models import Sum
from django.db.models import FloatField
from django.http import JsonResponse
from .models import Report

LEGAL_ARGS = {'columns', 'to_date', 'from_date', 'groupby', 'sortby',
              'country', 'os', 'channel'}

def get_report(request):
    '''
    Parses the URL called, extracts filters, groupers and sorters,
    and returns a report in JsonResponse format which can be easily
    cast to a pd.DataFrame.

    :param request: django.core.handlers.wsgi.WSGIRequest
    :return: django.http.JsonResponse
    '''

    check_for_illegal_args(request.GET)

    columns = extract_columns(request.GET)
    filters = extract_filters(request.GET)
    grouper = extract_groupby(request.GET)
    sorter = extract_sortby(request.GET)

    django_filters = tuples_to_django_filters(filters)
    queryset = Report.objects.filter(**django_filters)

    if grouper:
        queryset = groupby_queryset(queryset, grouper, columns)
    else:
        queryset = queryset.values(*columns.names)

    if sorter:
        queryset = sort_queryset(queryset, sorter)

    return JsonResponse(list(queryset), safe=False)

def check_for_illegal_args(querystring):
    '''
    :param querystring: django.http.request.QueryDict
    :return: ValueError, None
    '''

    illegal_args = querystring.keys() - LEGAL_ARGS
    if illegal_args:
        args = ", ".join(illegal_args)
        raise ValueError(f'Illegal arguments: {args}')

def sort_queryset(queryset, sorter):
    '''
    :param queryset: django.db.models.query.QuerySet
    :param sorter: collections.namedtuple
    :return: django.db.models.query.QuerySet
    '''

    how = "-" if sorter.how == "desc" else ""
    return queryset.order_by(how + sorter.column)

def groupby_queryset(queryset, grouper, columns):
    '''
    Groups the queryset based on grouper.columns,
    and aggregates values based on columns.names

    :param queryset: django.db.models.query.QuerySet
    :param grouper: collections.namedtuple
    :param columns: collections.namedtuple
    :return: django.db.models.query.QuerySet
    '''

    kwargs = {}
    if "cpi" in columns.names:
        kwargs['cpi'] = Sum('spend') / Sum('installs', output_field=FloatField())
        columns.names.remove('cpi')

    all_kwargs = {**kwargs, **{col: Sum(col) for col in columns.names}}
    grouped = queryset.values(*grouper.columns).annotate(**all_kwargs)
    return grouped

def extract_columns(params):
    '''
    Extracts desired columns from the querystring
    into a named tuple.
    :param params: django.http.request.QueryDict
    :return: collections.namedtuple
    '''

    Columns = namedtuple('Columns', 'names')

    try:
        columns = Columns(names=params.get('columns').split(","))
        return columns
    except AttributeError as e:
        raise Exception("No columns specified").with_traceback(e.__traceback__)

def extract_groupby(params):
    '''
    Extracts desired columns to group by,
    from the querystring into a named tuple.

    :param params: django.http.request.QueryDict
    :return: collections.namedtuple
    '''

    Grouper = namedtuple('Grouper', 'columns')
    try:
        grouper = Grouper(columns=params.get('groupby').split(","))
        return grouper
    except AttributeError as e:
        return

def extract_sortby(params):
    '''
    Extracts desired columns to sort by, and the method
    (desc, asc) from the querystring into a named tuple.

    :param params: django.http.request.QueryDict
    :return: collections.namedtuple
    '''

    Sorter = namedtuple('Sorter', 'column how')
    try:
        column, how = params.get('sortby').split(",")
    except AttributeError as err:
        # The sortby field wasn't specified
        return
    except ValueError as err:
        # wrongly formatted query
        raise err

    sorter = Sorter(column=column, how=how)
    return sorter

def str_to_date(date_string):
    '''
    Turns string date into a datetime.date object.

    :param date_string: str, string with date in format "2017-06-01".
    :return: datetime.date
    '''

    try:
        return datetime.date(*[int(x) for x in date_string.split("-")])
    except AttributeError:
        return
    except TypeError as err:
        raise err

def extract_filters(params):
    '''
    Extracts each of the filters specified in the url into a named tuple.

    :param params: django.http.request.QueryDict
    :return: list, list of named tuples representing each of the filters
    '''

    Filter = namedtuple('Filter', 'column value operator')

    from_date = Filter(column="date",
                       value=str_to_date(params.get('from_date')),
                       operator="gt")

    to_date = Filter(column="date",
                     value=str_to_date(params.get('to_date')),
                     operator="lt")

    channel = Filter(column="channel",
                     value=params.get('channel').split(",") if params.get('channel') else None,
                     operator="in")

    country = Filter(column="country",
                     value=params.get('country').split(",") if params.get('country') else None,
                     operator="in")

    opsys = Filter(column="os",
                   value=params.get('os').split(",") if params.get('os') else None,
                   operator="in")

    filters = [from_date, to_date, channel, country, opsys]
    filters = [filter for filter in filters if filter.value]
    return filters

def tuples_to_django_filters(tuples):
    '''
    Turns the named tuples into a dict format which can be then understood
    by django queryset.filter() function.
    :param tuples: list of named tuples
    :return: dict
    '''

    kwargs = {f'{tup.column}__{tup.operator}': tup.value for tup in tuples}
    return kwargs
