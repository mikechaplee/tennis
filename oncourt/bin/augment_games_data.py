#!home/mzc/anaconda2/bin/python

'''
Program to read OnCourt database tables (in csv form) and de-reference IDs
'''

import sys

PLAYERS_FILE = "players_%s.csv"
TOURS_FILE   = "tours_%s.csv"
GAMES_FILE   = "games_%s.csv"
ROUNDS_FILE  = "rounds.csv"

DEBUG = True

class BadHeaderError(Exception): pass

class IDToNameMapper(object):
    '''
    This ADT is the base-class for a bunch of CSV file-readers which seek to
    map a single ID to a name (e.g. player-ID to player-Name)
    It should be extended appropriately, see PlayerMapper for example
    
    It makes no assumption about where (in terms of position) the key + value
    are within a line - it requires the user to specify the column-names which
    correspond to the keys and values (NOT index positions)
    '''
    def __init__(self, wd, filename, idColumn, nameColumn):
        self._wd = wd                    # working directory
        self._file = filename            # set by derived class
        self._idColumn = idColumn        # key colname for our mapping
        self._nameColumn = nameColumn    # val colname for our mapping
        self._map = dict()               # our mapping: maps ID -> Name
    
    def getName(self, id):
        return self._map[id]
    
    def load(self):
        '''
        Reads the file and populates the map with ID -> Name mappings
        Returns: nothing
        Raises: BadHeaderError if header doesn't have necessary fields to map
                KeyError if duplicate IDs exist
        '''
        
        # this will be a list<str> of the file minus the header
        rawDataList = None 
        # these will be the column indices we'll want to rip out
        idPosition, namePosition = -1, -1 
        
        fname = "%s/%s" % (self._wd, self._file)
        if DEBUG: print "Mapper: Reading %s" % fname
        with open(fname , "r") as f:
            header = f.readline().strip()
            curIndex = 0
            for colName in header.split(","):
                if colName == self._idColumn:
                    idPosition = curIndex
                elif colName == self._nameColumn:
                    namePosition = curIndex
                curIndex += 1
            rawDataList = f.readlines()
        
        if idPosition == -1 or namePosition == -1:
            err = "idPos=%s, namePos=%s" % (idPosition, namePosition)
            raise BadHeaderError("Couldn't read header in %s: %s" %
                                                         (fname, err))
        if DEBUG: print "Mapper: idPosition=%s namePosition=%s" % (
                                 idPosition,   namePosition)
        
        for rawData in rawDataList:
            elements = rawData.strip().split(",")
            pID, pName = elements[idPosition], elements[namePosition]
            if pID and pName:
                self._map[pID] = pName # let this throw on dupes
        
        if DEBUG: print "Mapper: Loaded %s entries from %s" % (
                                        len(self._map), fname)

class PlayerMapper(IDToNameMapper):
    '''
    Simple mapper for players. Note the players-file is gender-specific
    '''
    def __init__(self, gender, wd, idColumn, nameColumn):
        super(PlayerMapper, self).__init__(wd,
                                           PLAYERS_FILE % gender,
                                           idColumn,
                                           nameColumn)

class TourMapper(IDToNameMapper):
    '''
    Simple mapper for tours. Note the tours-file is gender-specific
    '''
    def __init__(self, gender, wd, idColumn, nameColumn):
        super(TourMapper, self).__init__(wd,
                                         TOURS_FILE % gender,
                                         idColumn,
                                         nameColumn)

class RoundMapper(IDToNameMapper):
    '''
    Simple mapper for rounds (stages). Note the rounds-file is gender-neutral
    '''
    def __init__(self, wd, idColumn, nameColumn):
        super(RoundMapper, self).__init__(wd,
                                          ROUNDS_FILE,
                                          idColumn,
                                          nameColumn)

class AugmentedGamesfileGenerator(object):
    def __init__(self, gender, wd, playerMapper, tourMapper, roundMapper):
        self._wd   = wd
        self._file = GAMES_FILE % gender
        self._pmap = playerMapper
        self._tmap = tourMapper
        self._rmap = roundMapper
        self._games = list() # list<Game>
        self._header = ""

    class Game(object):
        '''
        Internal representation of a Game: must line up with games_<gender>.csv
        '''
        def __init__(self, winner, loser, tour, tround, result, gdate):
            self._winner = winner
            self._loser = loser
            self._tour = tour
            self._round = tround
            self._result = result
            self._gdate = gdate
        
        
        def toStr(self):
            return "%s,%s,%s,%s,%s,%s" % (
                    self._winner,
                    self._loser,
                    self._tour,
                    self._round,
                    self._result,
                    self._gdate)
        

    def load(self):
        '''
        This reads the games file, converting pesky IDs to names on the fly
        '''
        # this will be a list<Game> of the file minus the header
        gamesList = None 
        
        fname = "%s/%s" % (self._wd, self._file)
        if DEBUG: print "AugmentedGamesFileGenerator: Reading %s" % fname
        with open(fname , "r") as f:
            self._header = f.readline().strip()
            gamesList = f.readlines()
        
        for gameStr in gamesList: # game is a csv-string
            elements = gameStr.strip().split(",")
            game = AugmentedGamesfileGenerator.Game(
                        self._pmap.getName(elements[0]), # winner
                        self._pmap.getName(elements[1]), # loser
                        self._tmap.getName(elements[2]), # tour
                        self._rmap.getName(elements[3]), # round
                        elements[4], # result-string
                        elements[5]) # date-string
            self._games.append(game)
        if DEBUG: print "AugmentedGamesfileGenerator: Loaded %s games" % (
                                                             len(self._games))
        
    
    def dump(self, destPath):
        '''
        Writes the in-memory version of the file to disk
        '''
        if DEBUG: print "AugmentedGamesfileGenerator: Dumping to %s" % destPath
        outfile = open(destPath, "wb")
        outfile.write(self._header + "\n")
        outfile.write("\n".join([x.toStr() for x in self._games]))
        outfile.write("\n")
        outfile.close()
        if DEBUG: print "AugmentedGamesfileGenerator: Dump complete"
    
    
def doMain():
    genderCodes = ["atp", # men's
                   "wta"] # women's
    rawCsvDir = "/home/mzc/dev/tennis/oncourt/data/rawcsv" # source dir
    outCsvDir = "/home/mzc/dev/tennis/oncourt/data/csv"    # dest dir
    
    roundMapper = RoundMapper(rawCsvDir, "ID_R", "NAME_R")
    roundMapper.load()

    for gender in genderCodes:
        playerMapper = PlayerMapper(gender, rawCsvDir, "ID_P", "NAME_P")
        playerMapper.load()
    
        tourMapper = TourMapper(gender, rawCsvDir, "ID_T", "NAME_T")
        tourMapper.load()
    
    
        agg = AugmentedGamesfileGenerator(gender, rawCsvDir,
                                          playerMapper, tourMapper, roundMapper)
        agg.load()
        agg.dump("%s/augmented_games_%s.csv" % (outCsvDir, gender))
                              
    return 0

if __name__ == "__main__":
   doMain()