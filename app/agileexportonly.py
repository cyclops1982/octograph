#!/usr/bin/env python

from configparser import ConfigParser
from urllib import parse

import click
import maya
import requests

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def retrieve_paginated_data(
        api_key, url, from_date, to_date, page=None
):
    args = {
        'period_from': from_date,
        'period_to': to_date,
    }
    if page:
        args['page'] = page
    response = requests.get(url, params=args, auth=(api_key, ''))
    response.raise_for_status()
    data = response.json()
    results = data.get('results', [])
    if data['next']:
        url_query = parse.urlparse(data['next']).query
        next_page = parse.parse_qs(url_query)['page'][0]
        results += retrieve_paginated_data(
            api_key, url, from_date, to_date, next_page
        )
    return results


def store_agilerates(connection, agile_data):

    write_api = connection.write_api(write_options=SYNCHRONOUS)

    points = []
    for agile_rate in agile_data:
        print(f"{agile_rate['valid_from']} -> {agile_rate['valid_to']}: {agile_rate['value_inc_vat']}")
        points.append(Point("electricity").field("agile_rate", agile_rate['value_inc_vat']).time(agile_rate['valid_from']))
        if (len(points) > 48):
            write_api.write(bucket="energy", record=points)
            points=[]
    
    write_api.write(bucket="energy", record=points)

    




@click.command()
@click.option(
    '--config-file',
    default="octograph.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)
@click.option('--from-date', default='yesterday midnight', type=click.STRING)
@click.option('--to-date', default='tomorrow midnight', type=click.STRING)
def cmd(config_file, from_date, to_date):

    config = ConfigParser()
    config.read(config_file)

    influx = InfluxDBClient.from_config_file(config_file)

    api_key = config.get('octopus', 'api_key')
    if not api_key:
        raise click.ClickException('No Octopus API key set')

    agile_url = config.get('electricity', 'agile_rate_url', fallback=None)
    timezone = config.get('electricity', 'unit_rate_low_zone', fallback=None)


    from_iso = maya.when(from_date, timezone=timezone).iso8601()
    to_iso = maya.when(to_date, timezone=timezone).iso8601()

   
    click.echo(
        f'Retrieving Agile rates for {from_iso} until {to_iso}...',
        nl=False
    )
    agile_rates = retrieve_paginated_data( api_key, agile_url, from_iso, to_iso)
    click.echo(f' {len(agile_rates)} rates received.')
    store_agilerates(influx,  agile_rates)



if __name__ == '__main__':
    cmd()