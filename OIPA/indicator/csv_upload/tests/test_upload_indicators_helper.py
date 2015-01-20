import pytest
from geodata.factory import geodata_factory
from geodata.models import Country, City
# from django import setup
# setup()
from indicator.csv_upload import upload_indicators_helper


def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = list(funcarglist[0])
    metafunc.parametrize(argnames, [[funcargs[name] for name in argnames]
            for funcargs in funcarglist])

@pytest.mark.django_db
class TestUploadIndicatorHelper:

    # a map specifying multiple argument sets for a test method
    params = {
        'test_find_country': [
            dict(country_name="Netherlands", iso2="NL", expected="country_nl"),
            dict(country_name="Netherlands", iso2="", expected="country_nl"),
            dict(country_name="No country name", iso2="", expected=None),
        ],
        'test_find_city': [
            dict(country_name="Netherlands", iso2="NL", expected="country_nl"),
            dict(country_name="Netherlands", iso2="", expected="country_nl"),
            dict(country_name="No country name", iso2="", expected=None),
        ],
        'test_get_value': [
            dict(value_csv="1,33", expected="1.33"),
            dict(value_csv=" 1,33 ", expected="1.33"),
            dict(value_csv="NULL", expected=""),
            dict(value_csv="1.33", expected="1.33"),
            dict(value_csv="134134,03133", expected="134134.03133"),
        ]
    }

    def test_find_country(self, country_name, iso2, expected):

        country_nl = geodata_factory.CountryFactory.build(
            code='NL',
            name='Netherlands',
            alt_name='Nederland',
        )
        country_nl.save()

        if expected == "country_nl":
            expected = country_nl

        countries = Country.objects.all()

        result = upload_indicators_helper.find_country(country_name, countries, iso2)
        assert expected == result

    def test_find_city(self, city_name, , expected):

        city_ams = geodata_factory.CityFactory.build(
            code='NL',
            name='Netherlands',
            alt_name='Nederland',
        )
        city_ams.save()

        if expected == "city_ams":
            expected = city_ams

        countries = Country.objects.all()

        result = upload_indicators_helper.find_city(city_name, cities, country)
        assert expected == result

    def test_get_countries():

        country_nl = geodata_factory.CountryFactory.build(
            code='NL',
            name='Netherlands',
            alt_name='Nederland',
        )
        country_nl.save()

        countries = Country.objects.all()

    def test_get_cities():

        city_ams = geodata_factory.CityFactory.build(
            code='NL',
            name='Netherlands',
            alt_name='Nederland',
        )
        city_ams.save()

        cities = City.objects.all()

    def test_get_value(value_csv, expected):

        value = upload_indicators_helper.get_value(value_csv)
        assert expected == value


    def test_save_log():


    def test_save_city_data():



    def test_save_country_data():
