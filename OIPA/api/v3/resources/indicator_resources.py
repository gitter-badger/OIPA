# Tastypie specific
from tastypie.resources import ModelResource

# cache specific
from api.cache import NoTransformCache
from indicator.models import Indicator

# Direct sql specific
import ujson
from django.db import connection
from django.http import HttpResponse

# Helpers
from api.v3.resources.custom_call_helper import CustomCallHelper

class IndicatorDataResource(ModelResource):

    class Meta:
        #aid_type is used as dummy
        queryset = Indicator.objects.none()
        resource_name = 'indicator-data'
        include_resource_uri = True
        cache = NoTransformCache()
        allowed_methods = ['get']

    def add_indicator(self, geolocs, row, max_results):
        if not row['indicator_id'] in geolocs:
            max_value = max_results[0]['max_value']
            geolocs[row['indicator_id']] = {'indicator_friendly': row['friendly_label'],
                                          'type_data': row['type_data'],
                                          'indicator': row['indicator_id'],
                                          'category': row['category'],
                                          'selection_type': row['selection_type'],
                                          'max_value': max_value,
                                          'locs': {}}
        return geolocs

    def add_country_data(self, geolocs, c, longitude, latitude):
        geolocs[c['indicator_id']]['locs'][c['country_id']] = {'name': c['country_name'],
                                                               'id': c['country_id'],
                                                               'region_id': c['region_id'],
                                                               'longitude': longitude,
                                                               'latitude': latitude,
                                                               'years': {}}
        return geolocs

    def add_city_data(self, geolocs, r, longitude, latitude):
        geolocs[r['indicator_id']]['locs'][r['city_id']] = {'name': r['city_name'],
                                                            'id': r['city_id'],
                                                            'country_id': r['country_id'],
                                                            'region_id': r['region_id'],
                                                            'longitude': longitude,
                                                            'latitude': latitude,
                                                            'years': {}}
        return geolocs

    def get_city_data(self, filter_string):

        cursor = connection.cursor()
        cursor.execute('SELECT da.id as indicator_id, da.friendly_label, da.type_data, id.selection_type, da.category, '
                       'ci.name as city_name, r.code as region_id, r.name as region_name, c.code as country_id, '
                       'c.name as country_name, id.value, id.year, AsText(ci.location) as loc, ci.id as city_id '
                       'FROM indicator_indicatordata id '
                       'LEFT OUTER JOIN geodata_city ci ON id.city_id = ci.id '
                       'LEFT OUTER JOIN geodata_country c ON ci.country_id = c.code '
                       'LEFT OUTER JOIN geodata_region r ON c.region_id = r.code '
                       'LEFT OUTER JOIN indicator_indicator da ON da.id = id.indicator_id '
                       'WHERE id.country_id is null %s '
                       'ORDER BY id.value DESC' % filter_string)
        desc = cursor.description
        city_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ]
        return city_results

    def get_country_data(self, filter_string):

        cursor = connection.cursor()
        cursor.execute('SELECT da.id as indicator_id, da.friendly_label, id.selection_type, da.category, da.type_data, '
                       'r.code as region_id, r.name as region_name, c.code as country_id, c.name as country_name, '
                       'id.value, id.year, AsText(c.center_longlat) as loc '
                       'FROM indicator_indicatordata id '
                       'LEFT OUTER JOIN geodata_country c ON id.country_id = c.code '
                       'LEFT OUTER JOIN geodata_region r ON c.region_id = r.code '
                       'LEFT OUTER JOIN indicator_indicator da ON da.id = id.indicator_id '
                       'WHERE id.city_id is null %s '
                       'ORDER BY id.value DESC' % filter_string)
        desc = cursor.description
        country_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ]
        return country_results


    def get_list(self, request, **kwargs):
        helper = CustomCallHelper()
        city_q = helper.get_and_query(request, 'cities__in', 'city_id')
        country_q = helper.get_and_query(request, 'countries__in', 'c.code')
        region_q = helper.get_and_query(request, 'regions__in', 'r.code')
        year_q = helper.get_and_query(request, 'years__in', 'id.year')
        indicator_q = helper.get_and_query(request, 'indicators__in', 'indicator_id')
        selection_type_q = helper.get_and_query(request, 'selection_type__in', 'id.selection_type')
        limit_q = request.GET.get("limit", None)

        if limit_q:
            limit_q = int(limit_q)

        if not indicator_q and not country_q and not city_q:
            return HttpResponse(ujson.dumps("No indicator given"), content_type='application/json')

        # CITY DATA
        filter_string = 'AND (' + city_q + country_q + region_q + year_q + indicator_q + selection_type_q + ')'

        if 'AND ()' in filter_string:
            filter_string = filter_string[:-6]

        city_results = self.get_city_data(filter_string)

        # COUNTRY DATA
        filter_string = 'AND (' + city_q + country_q + region_q + year_q + indicator_q + selection_type_q + ')'

        if 'AND ()' in filter_string:
            filter_string = filter_string[:-6]

        country_results = self.get_country_data(filter_string)

        indicator_q = indicator_q.replace(" ) AND (", "")
        if indicator_q:
            indicator_q = "AND " + indicator_q

        cursor_max = connection.cursor()
        cursor_max.execute('SELECT indicator_id, max(value) as max_value '
                           'FROM indicator_indicatordata WHERE 1 %s '
                           'GROUP BY indicator_indicatordata.indicator_id '
                           'ORDER BY max_value DESC' % indicator_q)
        desc = cursor_max.description
        max_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor_max.fetchall()
        ]

        # REGION DATA
        # NOT IMPLEMENTED YET -> WE DO NOT HAVE CENTER LOCATIONS FOR REGIONS
        geolocs = {}

        for c in country_results:

            if c['value']:
                try:
                    geolocs[c['indicator_id']]['locs'][c['country_id']]['years']

                except:
                    geolocs = self.add_indicator(geolocs, c, max_results)

                    # if the amount of locs to be shown is reached, do not add the new loc
                    if limit_q:
                        if geolocs[c['indicator_id']]['locs'].__len__() == limit_q:
                            continue

                    loc = c['loc']
                    longitude = None
                    latitude = None
                    if loc:

                        loc = loc.replace("POINT(", "").replace(")", "").split(" ")
                        longitude = loc[0]
                        latitude = loc[1]

                    geolocs = self.add_country_data(geolocs, c, longitude=longitude, latitude=latitude)

                geolocs[c['indicator_id']]['locs'][c['country_id']]['years'][c['year']] = c['value']


        for r in city_results:

            if r['value']:
                try:
                    geolocs[r['indicator_id']]['locs'][r['city_id']]['years']

                except:
                    geolocs = self.add_indicator(geolocs, r, max_results)

                    # if the amount of locs to be shown is reached, do not add the new loc
                    if limit_q:
                        if geolocs[r['indicator_id']]['locs'].__len__() == limit_q:
                            continue

                    loc = r['loc']
                    longitude = None
                    latitude = None

                    if loc:
                        loc = loc.replace("POINT(", "").replace(")", "").split(" ")
                        longitude = loc[0]
                        latitude = loc[1]

                    self.add_city_data(geolocs, r, longitude, latitude)

                geolocs[r['indicator_id']]['locs'][r['city_id']]['years'][r['year']] = r['value']

        return HttpResponse(ujson.dumps(geolocs), content_type='application/json')




class IndicatorFilterOptionsResource(ModelResource):

    class Meta:
        #aid_type is used as dummy
        queryset = AidType.objects.all()
        resource_name = 'indicator-filter-options'
        include_resource_uri = True
        cache = NoTransformCache()
        allowed_methods = ['get']

    def get_list(self, request, **kwargs):
        helper = CustomCallHelper()
        city_q = helper.get_and_query(request, 'cities__in', 'city.id') or ""
        country_q = helper.get_and_query(request, 'countries__in', 'country.code') or ""
        region_q = helper.get_and_query(request, 'regions__in', 'region.code') or ""
        indicator_q = helper.get_and_query(request, 'indicators__in', 'i.indicator_id') or ""
        category_q = helper.get_and_query(request, 'categories__in', 'ind.category') or ""
        adm_division_q = request.GET.get("adm_division__in", "city,country,region") or ""
        adm_divisions = adm_division_q.split(",")

        filter_string = ' AND (' + city_q + country_q + region_q + indicator_q + category_q + ')'
        if 'AND ()' in filter_string:
            filter_string = filter_string[:-6]

        regions = {}
        countries = {}
        cities = {}
        indicators = {}
        jsondata = {}

        if "city" in adm_divisions:
            cursor = connection.cursor()
            # city filters
            cursor.execute('SELECT DISTINCT i.indicator_id, i.selection_type ,ind.friendly_label, '
                           'ind.category as indicator_category, city.id as city_id, city.name as city_name, '
                           'country.code as country_id, country.name as country_name, region.code as region_id, '
                           'region.name as region_name '
                           'FROM indicator_indicatordata i '
                           'JOIN indicator_indicator ind ON i.indicator_id = ind.id '
                           'JOIN geodata_city city ON i.city_id=city.id '
                           'LEFT OUTER JOIN geodata_country country on city.country_id = country.code '
                           'LEFT OUTER JOIN geodata_region region on country.region_id = region.code '
                           'WHERE 1 %s ' % (filter_string))

            desc = cursor.description
            city_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
            ]


            for r in city_results:

                region = {}
                if r['region_id']:
                    region[r['region_id']] = r['region_name']
                    regions.update(region)

                country = {}
                if r['country_id']:
                    country[r['country_id']] = r['country_name']
                    countries.update(country)

                city = {}
                if r['city_id']:
                    city[r['city_id']] = r['city_name']
                    cities.update(city)

                if r['indicator_id']:

                    if not r['indicator_id'] in indicators:
                        indicators[r['indicator_id']] = {"name": r['friendly_label'],
                                                         "category": r['indicator_category'], "selection_types": []}

                    if r['selection_type'] and r['selection_type'] not in indicators[r['indicator_id']]["selection_types"]:
                        indicators[r['indicator_id']]["selection_types"].append(r['selection_type'])


        if "country" in adm_divisions:
            # country filters
            filter_string = ' AND (' + country_q + region_q + indicator_q + category_q + ')'
            if 'AND ()' in filter_string:
                filter_string = filter_string[:-6]
            cursor = connection.cursor()
            cursor.execute('SELECT DISTINCT i.indicator_id, i.selection_type, ind.friendly_label, '
                           'ind.category as indicator_category, country.code as country_id, '
                           'country.name as country_name, region.code as region_id, region.name as region_name '
                           'FROM indicator_indicatordata i '
                           'JOIN indicator_indicator ind ON i.indicator_id = ind.id '
                           'JOIN geodata_country country on i.country_id = country.code '
                           'LEFT OUTER JOIN geodata_region region on country.region_id = region.code '
                           'WHERE 1 %s ' % (filter_string))

            desc = cursor.description
            country_results = [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
            ]

            for r in country_results:

                region = {}
                if r['region_id']:
                    region[r['region_id']] = r['region_name']
                    regions.update(region)

                country = {}
                if r['country_id']:
                    country[r['country_id']] = r['country_name']
                    countries.update(country)

                if r['indicator_id']:

                    if not r['indicator_id'] in indicators not in indicators[r['indicator_id']]["selection_types"]:
                        indicators[r['indicator_id']] = {"name": r['friendly_label'],
                                                         "category": r['indicator_category'], "selection_types": []}

                    if r['selection_type'] and r['selection_type'] not in indicators[r['indicator_id']]["selection_types"]:
                        indicators[r['indicator_id']]["selection_types"].append(r['selection_type'])


        if "region" in adm_divisions:
            # region filters
            filter_string = ' AND (' + region_q + indicator_q + category_q + ')'
            if 'AND ()' in filter_string:
                filter_string = filter_string[:-6]
            cursor = connection.cursor()
            cursor.execute('SELECT DISTINCT i.indicator_id, i.selection_type ,ind.friendly_label, '
                           'ind.category as indicator_category, region.code as region_id, region.name as region_name '
                           'FROM indicator_indicatordata i '
                           'JOIN indicator_indicator ind ON i.indicator_id = ind.id '
                           'JOIN geodata_region region on i.region_id = region.code '
                           'WHERE 1 %s ' % filter_string)

            desc = cursor.description
            region_results = [
                dict(zip([col[0] for col in desc], row))
                for row in cursor.fetchall()
            ]

            for r in region_results:

                region = {}
                if r['region_id']:
                    region[r['region_id']] = r['region_name']
                    regions.update(region)

                if r['indicator_id']:

                    if not r['indicator_id'] in indicators:
                        indicators[r['indicator_id']] = {"name": r['friendly_label'],
                                                         "category": r['indicator_category'], "selection_types": []}

                    if r['selection_type'] and r['selection_type'] not in indicators[r['indicator_id']]["selection_types"]:
                        indicators[r['indicator_id']]["selection_types"].append(r['selection_type'])

        jsondata['regions'] = regions
        jsondata['countries'] = countries
        jsondata['cities'] = cities
        jsondata['indicators'] = indicators

        return HttpResponse(ujson.dumps(jsondata), content_type='application/json')