# vim: set fileencoding=utf-8 :

import os.path
import re
import sqlite3
import time
import datetime

import requests


USERNAME = 'roberto@dealmeida.net'
PASSWORD = '9bp1dwjj'
DATABASE = 'inmet.db'


def create_tables(filename):
    """
    Create initial tables.

    """
    conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    curs = conn.cursor()

    curs.execute('''CREATE TABLE stations (
            code integer,
            name text,
            latitude real,
            longitude real,
            altitude real, 
            operating integer,
            start date,
            updated date)''')

    # names according to CF standard names table:
    # http://cf-pcmdi.llnl.gov/documents/cf-standard-names/standard-name-table/19/cf-standard-name-table.html
    curs.execute('''CREATE TABLE records (
            station integer,
            datetime timestamp,
            precipitation_amount real,
            dry_bulb_temperature real,
            wet_bulb_temperature real,
            max_air_temperature real,
            min_air_temperature real,
            relative_umidity real,
            air_pressure real,
            air_pressure_at_sea_level real,
            wind_from_direction real,
            wind_speed real,
            insolation real)''')

    conn.commit()
    curs.close()


def parse_file(content):
    """
    Parse the content of the data response.

    The response is in HTML, with the actual data inside the first `<pre>`. 
    Here's an example snippet:

        --------------------
        BDMEP - INMET
        --------------------
        Estação           : SAO PAULO  IAG  - SP (OMM: 83004)
        Latitude  (graus) : -23.65
        Longitude (graus) : -46.61
        Altitude  (metros): 800.00
        Estação Operante
        Inicio de operação: 01/01/1933
        Periodo solicitado dos dados: 01/01/1900 a 31/12/2013
        Os dados listados abaixo são os que encontram-se digitados no BDMEP
        --------------------
        Obs.: Os dados aparecem separados por ; (ponto e vírgula) no formato txt.
              Para o formato planilha XLS, siga as instruções
        --------------------
        Estacao;Data;Hora;Precipitacao;TempBulboSeco;TempBulboUmido;TempMaxima;TempMinima;UmidadeRelativa;PressaoAtmEstacao;PressaoAtmMar;DirecaoVento;VelocidadeVentoInsolacao;
        83004;01/01/1995;0000;;;;26.8;;;;;;;1.5;
        83004;01/01/1995;1200;21.2;22.5;20;;19.5;80;924.6;;32;4;;
        83004;01/01/1995;1800;;25.2;21.5;;;73;922.9;;32;2;;
        ...

    """
    content = re.search('<pre>(.*?)</pre>', content, re.DOTALL).group(1)
    empty, header, metadata, obs, data = content.split('--------------------')

    # process station
    station = {}
    metadata = metadata.strip().split('\n')
    station['name'] = re.sub(' \(OMM: \d+\)', '', metadata[0].split(':', 1)[1].strip())
    station['latitude'] = float(metadata[1].split(':', 1)[1].strip())
    station['longitude'] = float(metadata[2].split(':', 1)[1].strip())
    station['altitude'] = float(metadata[3].split(':', 1)[1].strip())
    station['operating'] = metadata[4].strip() == u'Estação Operante'
    station['start'] = datetime.date(*time.strptime(metadata[5].split(':', 1)[1].strip(), '%d/%m/%Y')[:3])

    # process data
    data = data.strip().split('\n')
    vars_ = data.pop(0)
    def process(line):
        line = line.split(';')
        return [
                int(line[0]),
                datetime.datetime.strptime(line[1]+line[2], '%d/%m/%Y%H%M'),
                line[3] and float(line[3]) or None,
                line[4] and (float(line[4]) + 274.15) or None,  # deg C => K
                line[5] and (float(line[5]) + 274.15) or None,  # deg C => K
                line[6] and (float(line[6]) + 274.15) or None,  # deg C => K
                line[7] and (float(line[7]) + 274.15) or None,  # deg C => K
                line[8] and (float(line[8]) / 100.) or None,    # %     => 1
                line[9] and (float(line[9]) * 100.) or None,    # mbar  => Pa
                line[10] and (float(line[10]) * 100.) or None,  # mbar  => Pa
                line[11] and get_direction(line[11]) or None,
                line[12] and float(line[12]) or None,
                line[13] and float(line[13]) or None,
        ]
    data = map(process, data)

    return station, data


def get_direction(code):
    """
    Return approximate wind direction from code.

    """
    try:
        return {
                1: 1, 2: 1, 3: 1, 4: 1,
                5: 2, 6: 2,
                7: 3, 8: 3,
                9: 4, 10: 4,
                11: 5, 12: 5, 13: 5,
                14: 6, 15: 6,
                16: 7, 17: 7,
                18: 8, 19: 8,
                20: 9, 21: 9, 22: 9,
                23: 10, 24: 10,
                25: 11, 26: 11,
                27: 12, 28: 12,
                29: 13, 30: 13, 31: 13,
                32: 14, 33: 14,
                34: 15, 35: 15,
                36: 0,
        }[int(code)] * 22.5
    except:
        return None


def get_cookie():
    """
    Return the session cookie for further requests.

    We need to POST to this URL:

        http://www.inmet.gov.br/projetos/rede/pesquisa/inicio.php

    The following form data:

        mUsuario:
        mGerModulo:mGerModulo
        mCod:USERNAME
        mSenha:PASSWORD
        mGerModulo:mGerModulo
        btnProcesso: Acessar 

    then get the cookie `PHPSESSID`.

    """
    r = requests.post(
        'http://www.inmet.gov.br/projetos/rede/pesquisa/inicio.php',
        data={
            'mUsuario'   : '',
            'mGerModulo' : 'mGerModulo',
            'mCod'       : USERNAME,
            'mSenha'     : PASSWORD,
            'mGerModulo' : 'mGerModulo',
            'btnProcesso': ' Acessar ',
        })
    return r.cookies['PHPSESSID']


def get_data(code, start, end, session=None):
    if session is None:
        session = get_cookie()

    r = requests.get(
        'http://www.inmet.gov.br/projetos/rede/pesquisa/gera_serie_txt.php',
        params={
            'mRelEstacao' : code,
            'btnProcesso' : 'serie',
            'mRelDtInicio': start.strftime('%d/%m/%Y'),
            'mRelDtFim'   : end.strftime('%d/%m/%Y'),
            'mAtributos'  : '1,1,1,1,1,1,1,1,1,1,1',
        },
        cookies={
            'PHPSESSID': session,
        })
    return parse_file(r.text)


if __name__ == '__main__':
    # check if we need to create the database
    if not os.path.exists(DATABASE):
        print 'Creating database %s.' % DATABASE
        create_tables(DATABASE)
    conn = sqlite3.connect(DATABASE, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    curs = conn.cursor()

    session = get_cookie()

    with open('stations.txt') as stations:
        for code in stations:
            code = int(code)
            print '=' * 80
            print 'Processing station %d.' % code

            curs.execute("SELECT operating, updated FROM stations WHERE code=?", (code,))
            results = curs.fetchone()
            if results:
                operating, updated = results
                if not operating:
                    print 'Station is deactivated, skipping.'
                    continue
                print 'Station was last updated %s.' % updated
            else:
                print 'First time this station is downloaded.'
                updated = datetime.date(1900, 1, 1)

            today = datetime.date.today()
            if today == updated:
                print 'Nothing to retrieve.'
                continue
            print 'Retrieving data from %s to %s.' % (updated, today)
            try:
                station, data = get_data(code, updated, today, session)
            except:
                continue

            # add data
            print 'Inserting data...'
            curs.executemany('''INSERT INTO records
                (station, datetime, precipitation_amount, dry_bulb_temperature,
                wet_bulb_temperature, max_air_temperature, min_air_temperature, 
                relative_umidity, air_pressure, air_pressure_at_sea_level, 
                wind_from_direction, wind_speed, insolation) VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)

            # update station
            station['code'] = code
            station['updated'] = today
            if results:
                print 'Updating station status.'
                curs.execute('''UPDATE stations SET 
                        name=:name,
                        latitude=:latitude, longitude=:longitude, altitude=:altitude,
                        operating=:operating, start=:start, updated=:updated
                        WHERE code=:code''', station)
            else:
                print 'Creating station status.'
                curs.execute('''INSERT INTO stations 
                    (code, name, latitude, longitude, altitude, operating, start, updated)
                    VALUES
                    (:code, :name, :latitude, :longitude, :altitude, :operating, :start, :updated)''', station)

            conn.commit()
