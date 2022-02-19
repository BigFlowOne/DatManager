#!/usr/bin/env python3
import os
import os.path
import hashlib
import xml.etree.ElementTree as ET
import codecs
import sys
import mysql.connector
from mysql.connector import errorcode
import argparse
import lxml.etree
import configparser


class Config:
    def __init__(self, appdir):
        self.appdir = appdir
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(appdir, 'data/config.ini'))
        self.host = self.config['mysql']['host']
        self.user = self.config['mysql']['user']
        self.password = self.config['mysql']['password']
        self.db = self.config['mysql']['db']


class DBinit:
    def __init__(self, config):
        self.user = config.user
        self.password = config.password
        self.host = config.host
        self.database = config.db
        try:
            self.mysqlcon = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                                                    database=self.database)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def closemysql(self):
        self.mysqlcon.close()


class Romfiles:
    def __init__(self, rom_id, name, filename, type, size, crc, md5, sha1, matchcode):
        self.rom_id = rom_id
        self.name = name
        self.filename = filename
        self.type = type
        self.size = size
        self.crc = crc
        self.md5 = md5
        self.sh1 = sha1
        self.matchcode = matchcode
        self.romfileid = None
        self.indb = None
        self.existsindb()

    @classmethod
    def import_from_xml(cls, rom_id, game, rom, matchcode):
        name = game.attrib['name'][:game.attrib['name'].rfind('.')]
        filename = game.attrib['name']
        datatype = rom.attrib['name'].rsplit('.', 1)[-1]
        return cls(rom_id, name, filename, datatype, rom.attrib['size'], rom.attrib['crc'], rom.attrib['md5'],
                   rom.attrib['sha1'], matchcode)

    def existsindb(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = """SELECT if(COUNT(dtdrm_files.matchcode)>=1,TRUE,FALSE) FROM dtdrm_files WHERE dtdrm_files.matchcode = %s"""
        cursor.execute(query, (self.matchcode,))
        result = cursor.fetchone()
        self.indb = result[0]
        return self.indb

    def writetodb(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = "INSERT INTO dtdrm_files (rom_id, name, filename, type, size, crc, md5, sha1, matchcode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (self.rom_id, self.name, self.filename, self.type, self.size, self.crc, self.md5, self.sh1,
                  self.matchcode)
        cursor.execute(query, values)
        DB.mysqlcon.commit()

    def deleteindbbymatchcode(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = """Delete from dtdrm_files where matchcode = %s"""
        values = (self.matchcode)
        cursor.execute(query, values)
        DB.mysqlcon.commit()


class Rom:
    def __init__(self, sys_id, name, category, description, matchcode, romfiles=None):
        self.sys_id = sys_id
        self.name = name
        self.category = category
        self.description = description
        self.matchcode = matchcode
        if romfiles is None:
            self.romfiles = []
        else:
            self.romfiles = romfile
        self.rom_id = None
        self.indb = None
        self.existsindb()

    def writetodb(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = "INSERT INTO dtdrm_rom (sys_id, name, category, description, matchcode) VALUES (%s, %s, %s, %s, %s)"
        values = (self.sys_id, self.name, self.category, self.description, self.matchcode)
        cursor.execute(query, values)
        DB.mysqlcon.commit()
        self.rom_id = cursor.lastrowid
        return self.rom_id

    def existsindb(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = """SELECT if(COUNT(dtdrm_rom.matchcode)>=1,TRUE,FALSE) FROM dtdrm_rom WHERE dtdrm_rom.matchcode = %s"""
        cursor.execute(query, (self.matchcode,))
        result = cursor.fetchone()
        self.indb = result[0]
        return self.indb

    def deleteindbbymatchcode(self):
        self.deleteallromfilesindb()
        global DB
        cursor = DB.mysqlcon.cursor()
        query = """Delete from dtdrm_rom where matchcode = %s"""
        values = (self.matchcode)
        cursor.execute(query, values)
        DB.mysqlcon.commit()

    def deleteallromfilesindb(self):
        for romfile in self.romfiles:
            romfile.deleteindbbymatchcode()

    def add_romfile(self, romfile):
        if romfile not in self.romfiles:
            self.romfiles.append(romfile)

    def remove_romfile(self, romfile):
        if romfile in self.romfiles:
            self.romfiles.remove(romfile)

    @classmethod
    def import_from_xml(cls, sys_id, game):
        return cls(sys_id, game.attrib['name'], game.find('category').text, game.find('description').text,
                   genmatchcodefromxml(game))


class Datfile:
    def __init__(self, direkt, path, type=".dat"):
        self.direkt = direkt
        self.path = path
        self.type = type
        if not direkt:
            for file in os.listdir(path):
                if file.endswith(type):
                    self.file = os.path.join(path, file)
        else:
            self.file = path


class RomSystem:
    def __init__(self, name, description, version, date, author, homepage, url, roms=None):
        self.name = name
        self.description = description
        self.version = version
        self.date = date
        self.author = author
        self.homepage = homepage
        self.url = url
        self.sys_id = None
        if roms is None:
            self.roms = []
        else:
            self.roms = roms
        self.indb = None
        self.existsindb()

    def writetodb(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = "INSERT INTO dtdrm_sys (name, description, version, date, author, homepage, url) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        values = (self.name, self.description, self.version, self.date, self.author, self.homepage, self.url)
        cursor.execute(query, values)
        DB.mysqlcon.commit()
        self.sys_id = cursor.lastrowid
        return self.sys_id

    def deleteindbbyname(self):
        self.deleteallromsindb()
        global DB
        cursor = DB.mysqlcon.cursor()
        query = """Delete from dtdrm_rom where name LIKE %s"""
        values = (self.name)
        cursor.execute(query, values)
        DB.mysqlcon.commit()

    def deleteallromsindb(self):
        for rom in self.roms:
            rom.deleteindbbymatchcode()

    def add_rom(self, rom):
        if rom not in self.roms:
            self.roms.append(rom)

    def remove_rom(self, rom):
        if rom in self.roms:
            self.roms.remove(rom)

    def existsindb(self):
        global DB
        cursor = DB.mysqlcon.cursor()
        query = """SELECT if(COUNT(dtdrm_sys.id)>=1,TRUE,FALSE) FROM dtdrm_sys WHERE dtdrm_sys.name LIKE %s"""
        cursor.execute(query, (self.name,))
        result = cursor.fetchone()
        self.indb = result[0]
        return self.indb

    @classmethod
    def import_from_xml(cls, root):
        head = {}
        header = root.find('header')
        head["name"] = header.find('name').text
        head["description"] = header.find('description').text
        head["version"] = header.find('version').text
        head["date"] = header.find('date').text
        head["author"] = header.find('author').text
        head["homepage"] = header.find('homepage').text
        head["url"] = header.find('url').text
        return cls(head["name"], head["description"], head["version"], head["date"], head["author"], head["homepage"],
                   head["url"])


def genmatchcodefromxml(game):
    matchcode = ""
    for rom in game.iter('rom'):
        if ".cue" not in rom.attrib['name']:
            matchcode += rom.attrib['md5']
    matchcode = hashlib.md5(matchcode.encode('utf-8')).hexdigest()
    return matchcode


def importdat(file):
    stepc = lxml.etree.parse(file)
    l = stepc.xpath('count(//rom)')
    xml = ET.parse(file)
    root = xml.getroot()
    romsys = RomSystem.import_from_xml(root)
    sys_id = romsys.writetodb()

    ##printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
    i = 0
    if romsys.indb:
        print("im sys")
    else:
        print("nicht im sys")
    for game in root.findall('game'):
        rom = Rom.import_from_xml(sys_id, game)
        romsys.add_rom(rom)
        rom_id = rom.writetodb()
        for romdata in game.iter('rom'):
            i += 1
            romfile = Romfiles.import_from_xml(rom_id, game, romdata, rom.matchcode)
            rom.add_romfile(romfile)
            ### das ist ein Test

            if romfile.indb:
                romfile.writetodb()
            else:
                romfile.writetodb()

            ##printProgressBar(i, l, prefix='Progress:', suffix='Complete', length=50)
    print("Import completed successfully: " + romsys.name + " (" + romsys.description + ")")


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    if iteration == total:
        print()


# Pfad erkennen
frozen = 'not'
if getattr(sys, 'frozen', False):
    appdir = os.path.dirname(os.path.abspath(sys.executable))
else:
    appdir = os.path.dirname(os.path.abspath(__file__))

parser = argparse.ArgumentParser(description='Dat Manager')
parser.add_argument("-i", "--importdat", dest='importdat', help="for import", action='store_true')
parser.add_argument("-f", "--file", help="Dat input file", type=argparse.FileType('r'))
parser.add_argument('--version', action='version', version='%(prog)s 1.0')

cfg = Config(appdir)
DB = DBinit(cfg)

results = parser.parse_args()
if results.importdat:
    if results.file:
        dat = Datfile(True, results.file.name)
        importdat(dat.file)
        results.file.close()
    else:
        print("Load .dat File from import Folder ...")
        dat = Datfile(False, "import", ".dat", )
        importdat(dat.file)

DB.closemysql()
