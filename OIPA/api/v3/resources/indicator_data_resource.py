from tastypie.resources import ModelResource
from api.cache import NoTransformCache
from indicator.models import Indicator
import ujson
from django.db import connection
from django.http import HttpResponse
from api.v3.resources.custom_call_helper import CustomCallHelper
from django.contrib.gis.geos import fromstr

class IndicatorDataResource(ModelResource):

    class Meta:
        queryset = Indicator.objects.none()
        resource_name = 'indicator-data'
        include_resource_uri = True
        cache = NoTransformCache()
        allowed_methods = ['get']

    def get_city_data_from_db(self, filter_string):

        cursor = connection.cursor()
        cursor.execute('SELECT da.id as indicator_id, da.friendly_label, da.type_data, id.selection_type, da.category, '
                       'ci.name as city_name, r.code as region_id, r.name as region_name, c.code as country_id, '
                       'c.name as country_name, idv.value, idv.year, AsText(ci.location) as loc, ci.id as city_id '
                       'FROM indicator_indicatordata id '
                       'LEFT OUTER JOIN geodata_city ci ON id.city_id = ci.id '
                       'LEFT OUTER JOIN geodata_country c ON ci.country_id = c.code '
                       'LEFT OUTER JOIN geodata_region r ON c.region_id = r.code '
                       'LEFT OUTER JOIN indicator_indicator da ON da.id = id.indicator_id '
                       'LEFT OUTER JOIN indicator_indicatordatavalue idv ON id.id = idv.indicator_data_id '
                       'WHERE id.country_id is null %s '
                       'ORDER BY idv.value DESC' % filter_string)
        desc = cursor.description
        city_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ]
        return city_results

    def get_country_data_from_db(self, filter_string):

        cursor = connection.cursor()
        cursor.execute('SELECT da.id as indicator_id, da.friendly_label, id.selection_type, da.category, da.type_data, '
                       'r.code as region_id, r.name as region_name, c.code as country_id, c.name as country_name, '
                       'idv.value, idv.year, AsText(c.center_longlat) as loc '
                       'FROM indicator_indicatordata id '
                       'LEFT OUTER JOIN geodata_country c ON id.country_id = c.code '
                       'LEFT OUTER JOIN geodata_region r ON c.region_id = r.code '
                       'LEFT OUTER JOIN indicator_indicator da ON da.id = id.indicator_id '
                       'LEFT OUTER JOIN indicator_indicatordatavalue idv ON id.id = idv.indicator_data_id '
                       'WHERE id.city_id is null %s '
                       'ORDER BY idv.value DESC' % filter_string)
        desc = cursor.description
        country_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ]
        return country_results

    def get_filter_string(self, request):

        helper = CustomCallHelper()
        city_q = helper.get_and_query(request, 'cities__in', 'city_id')
        country_q = helper.get_and_query(request, 'countries__in', 'c.code')
        region_q = helper.get_and_query(request, 'regions__in', 'r.code')
        year_q = helper.get_and_query(request, 'years__in', 'id.year')
        selection_type_q = helper.get_and_query(request, 'selection_type__in', 'id.selection_type')
        indicator_q = helper.get_and_query(request, 'indicators__in', 'indicator_id')

        filter_string = 'AND (' + city_q + country_q + region_q + year_q + indicator_q + selection_type_q + ')'

        if 'AND ()' in filter_string:
            filter_string = " "

        return filter_string

    def get_max_value(self, indicator_q):

        indicator_q = indicator_q.replace(" ) AND (", "")
        cursor_max = connection.cursor()
        cursor_max.execute(
            'SELECT indicator_id, max(value) as max_value '
            'FROM indicator_indicatordatavalue as idv '
            'JOIN indicator_indicatordata as id on idv.indicator_data_id = id.id '
            'WHERE 1 AND %s '
            'GROUP BY indicator_id '
            'ORDER BY max_value DESC' % indicator_q)
        desc = cursor_max.description
        max_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor_max.fetchall()
        ]

        max_value = max_results[0]['max_value']
        return max_value

    def get_latlng(self, loc):

        if loc:
            return fromstr(loc)
        else:
            return [None, None]

    def get_indicator_from_results(self, row, max_value):
        return {
            'indicator_friendly': row['friendly_label'],
            'type_data': row['type_data'],
            'indicator': row['indicator_id'],
            'category': row['category'],
            'selection_type': row['selection_type'],
            'max_value': max_value,
            'locs': {}}

    def get_country_data_from_results(self, row, longitude, latitude):
         return {
             'name': row['country_name'],
             'id': row['country_id'],
             'region_id': row['region_id'],
             'longitude': longitude,
             'latitude': latitude,
             'years': {}}

    def get_city_data_from_results(self, row, longitude, latitude):
        return {
            'name': row['city_name'],
            'id': row['city_id'],
            'country_id': row['country_id'],
            'region_id': row['region_id'],
            'longitude': longitude,
            'latitude': latitude,
            'years': {}}

    def add_country_row(self, output, row, limit_q, max_value):

        if row['value']:

            try:
                output[row['indicator_id']]['locs'][row['country_id']]['years']

            except:
                if not row['indicator_id'] in output:
                    output[row['indicator_id']] = self.get_indicator_from_results(row, max_value)

                if limit_q:
                    if output[row['indicator_id']]['locs'].__len__() == int(limit_q):
                        return False

                loc = self.get_latlng(row['loc'])

                if not row['country_id'] in output[row['indicator_id']]['locs']:
                    output[row['indicator_id']]['locs'][row['country_id']] = self.get_country_data_from_results(
                        row,
                        longitude=loc[0],
                        latitude=loc[1])

            output[row['indicator_id']]['locs'][row['country_id']]['years'][row['year']] = row['value']

        return output

    def add_city_row(self, output, row, limit_q, max_value):

        if row['value']:

            try:
                output[row['indicator_id']]['locs'][row['city_id']]['years']

            except:
                if not row['indicator_id'] in output:
                    output[row['indicator_id']] = self.get_indicator_from_results(row, max_value)

                # if the amount of locs to be added is reached, do not add the new loc
                if limit_q and output[row['indicator_id']]['locs'].__len__() == limit_q:
                    return False

                loc = self.get_latlng(row['loc'])

                if not row['city_id'] in output[row['indicator_id']]['locs']:
                    output[row['indicator_id']]['locs'][row['city_id']] = self.get_city_data_from_results(
                        row=row,
                        longitude=loc[0],
                        latitude=loc[1])

            output[row['indicator_id']]['locs'][row['city_id']]['years'][row['year']] = row['value']

        return output

    def get_list(self, request, **kwargs):

        helper = CustomCallHelper()
        city_q = helper.get_and_query(request, 'cities__in', 'city_id')
        country_q = helper.get_and_query(request, 'countries__in', 'c.code')
        indicator_q = helper.get_and_query(request, 'indicators__in', 'indicator_id')
        limit_q = request.GET.get("limit", None)

        if not indicator_q and not country_q and not city_q:
            return HttpResponse(ujson.dumps("No indicator given"), content_type='application/json')

        filter_string = self.get_filter_string(request)
        city_results = self.get_city_data_from_db(filter_string)
        country_results = self.get_country_data_from_db(filter_string)
        max_value = self.get_max_value(indicator_q)

        output = {}

        for row in country_results:
            output = self.add_country_row(output, row, limit_q, max_value)

        for row in city_results:
            output = self.add_city_row(output, row, limit_q, max_value)

        return HttpResponse(ujson.dumps(output), content_type='application/json')