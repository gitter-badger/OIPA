from django.contrib.admin.sites import AdminSite
from indicator.admin import IndicatorDataUploadAdmin
from indicator.csv_upload import upload_indicators_helper
from multiupload.admin import MultiUploadAdmin
from django.test import RequestFactory
from django.db import models
from unittest import TestCase

from indicator.models import Indicator

import StringIO
from django.core.files.uploadedfile import InMemoryUploadedFile



import pytest


class TestMultiUploadParser(TestCase):



    @pytest.mark.parametrize("input,expected", [
        ('descriptions,titles,identifiers', ['descriptions', 'titles', 'identifiers']), #should work when the param contains no whitespace
        ('descriptions, titles, identifiers', ['descriptions', 'titles', 'identifiers']), #should work when the param contains whitespace after ','
        ('    descriptions    ,    titles    ,   identifiers'   , ['descriptions', 'titles', 'identifiers']), #should work when the param contains unexpected whitespace
    ])



    def test_comma_separated_parameter_to_list(input, expected):
	    assert comma_separated_parameter_to_list(input) == expected


    def test_country_find(self, ):

        upload_indicators_helper.find_country("Netherlands", "")
        find_country


        # not implemented
        assert False

    def test_city_find(self):
        # not implemented
        assert False


    def test_csv_parse(self):
        """
        rest of the uploading process is handled by multiupload lib

        """

        def get_temporary_text_file():
            io = StringIO.StringIO()
            io.write('foo')
            text_file = InMemoryUploadedFile(io, None, 'foo.txt', 'text', io.len, None)
            text_file.seek(0)
            return text_file

        indicator_data_upload_admin = IndicatorDataUploadAdmin(MultiUploadAdmin, AdminSite())

        # create csv file
        test_file = get_temporary_text_file()
        request_dummy = RequestFactory().get('/')

        indicator_data_upload_admin.process_uploaded_file(
            uploaded=self.test_file,
            object=None,
            request=self.request_dummy,
            kwargs={})


        Indicator.objects.all()

        assert True
        # TODO assert true if the indicator from the csv exists






#
# from django.test import RequestFactory
# from iati.factory import iati_factory
# from api.sector import serializers
#
#
# class TestSectorSerializers:
#
#     request_dummy = RequestFactory().get('/')
#
#     def test_SectorSerializer(self):
#         sector = iati_factory.SectorFactory.build(
#             code=10,
#             name='Sector A',
#             description='Description A'
#         )
#         serializer = serializers.SectorSerializer(
#             sector,
#             context={'request': self.request_dummy}
#         )
#         assert serializer.data['code'] == sector.code, \
#             """
#             the data in sector.code should be serialized to a field named code
#             inside the serialized object
#             """
#         assert serializer.data['name'] == sector.name, \
#             """
#             the data in sector.name should be serialized to a field named code
#             inside the serialized object
#             """
#         assert serializer.data['description'] == sector.description, \
#             """
#             the data in sector.description should be serialized to a field named code
#             inside the serialized object
#             """
#         required_fields = (
#             'url',
#             'activities',
#             'category'
#         )
#         assertion_msg = "the field '{0}' should be in the serialized sector"
#         for field in required_fields:
#             assert field in serializer.data, assertion_msg.format(field)
#
#
#
#
#
# import pytest
# from api.v3.tests.endpoint_base import EndpointBase
#
#
# @pytest.mark.django_db
# class TestEndpoints(EndpointBase):
#
#     def test_activities_endpoint(self, client, activity):
#         response = client.get('/api/v3/activities/')
#         assert response.status_code == 200
