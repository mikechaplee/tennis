#!/home/mzc/anaconda2/bin/python

'''
Program to read OnCourt database tables (in csv form) and de-reference IDs,
do some merging of tables, and filtering.

Notes:
 - Data layout of original CSVs:
     players_atp.csv: player IDs and personal data (date-of-birth, country etc)
     games_atp.csv: contains match summary info in terms of 4 important IDs:
       - winner/loser ID
       - tournament ID (see note on tours_atp.csv below)
       - round ID ('Semi-finals' etc)
     stat_atp.csv:  match stats with the same 4 IDs to link with games_atp.csv
     tours_atp.csv: maps IDs to tournament info (ID for wimbledon 2014 != 2015)
     rounds.csv: maps IDs to First, Quater-Final etc
     courts.csv: maps IDs to court surface types

 - The aim of this program is to analyse the above files and create a single
   augmented file with all the data we need on each match, in a more human-
   readable form (i.e. all numeric IDs from files above are dereferenced). If
   we need to find our way back / forth between our augmented data and the raw
   CSVs, the final column of each row in our augmented file is the 4-part ID
   discussed above (and modelled in the MatchKey class of this program). This
   is useful for speeding up investigations where data translation may be buggy

 - we deliberately drop doubles matches
 
 - sometimes the match-date is missing from the data: we flag this to log & use
    tournatment date info

 - sometimes all stats info is missing for a match: we fill all stats columns
   with 'n/a'

 - sometimes some stats info is available for a match, but not all of the 
   stats were recorded - we put in 0's for the missing ones

 - suspicious looking stats go into the final 'Suspect Columns' column

 - we calculate player ages, but date-of-birth info isn't available for all
   players; we exclude matches they're involved in. this costs us less than
   8% of our dataset for men's (atp) matches.

'''

PLAYERS_FILE = "players_%s.csv"
TOURS_FILE   = "tours_%s.csv"
GAMES_FILE   = "games_%s.csv"
STATS_FILE   = "stat_%s.csv"

ROUNDS_FILE  = "rounds.csv"
COURTS_FILE  = "courts.csv"

DEBUG = True

class BadHeaderError(Exception): pass

def convertOnCourtDateToYmd(dateStr):
    '''
    dateStr looks like MM/DD/YY HH:MM:SS
    We convert it to YYYY/MM/DD
    '''
    dateParts = dateStr.split(" ")[0].split("/")
    year = int(dateParts[2])
    return "%s/%s/%s" % (
                    year+1900 if year >= 45
                              else year + 2000,
                    dateParts[0],
                    dateParts[1])

class MatchKey(object):
    '''
    Key to uniquely identify a match in the raw CSV files
    from OnCourt DB extraction. The name field is the
    start of the rows in games_<gender>.csv files and
    stat_<gender>.csv as well
    '''
    def __init__(self, winnerID, loserID, tourID, roundID):
        '''
        No args can be None or ValueError will be thrown
        '''
        self._winnerID = winnerID
        self._loserID = loserID
        self._tourID = tourID
        self._roundID = roundID
        ''' name is just composite of attrs - but might
        as well create it now while those are hot in cache
        '''
        self._name = "%s/%s/%s/%s" % (
                winnerID, loserID, tourID, roundID)
        if not winnerID or not loserID or not tourID or not roundID:
            raise ValueError("Bad MatchKey formed: %s" % self._name)

    @property
    def Name(self):
        return self._name

    def __hash__(self):
        return hash(self._name)

    def __eq__(self, other):
        return self._name == other._name

    def __ne__(self, other):
        return not(self == other)

    def __str__(self):
        return self._name


class IDToNameMapper(object):
    '''
    This ADT is the base-class for a bunch of CSV file-readers which seek to
    map a single ID to a name (e.g. player-ID to player-Name)
    It should be extended appropriately, see RoundMapper for example
    
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
            elements = rawData.replace('"','').strip().split(",")
            pID, pName = elements[idPosition], elements[namePosition]
            if pID and pName:
                self._map[pID] = pName # let this throw on dupes
                        
        
        if DEBUG: print "Mapper: Loaded %s entries from %s" % (
                                        len(self._map), fname)

class PlayerInfo(object):
    def __init__(self, _id, name, dob):
        ''' all fields public, life's too short '''
        self.id = _id
        self.name = name
        self.dob = convertOnCourtDateToYmd(dob)

    def getAgeAsOf(self, asOfDateStr):
        '''
        Returns the age of this player on date asOfDateStr
        in units of years (as float). Note that asOfDateStr
        must be in Y-m-d format
        '''
        from datetime import datetime
        return (datetime.strptime(asOfDateStr, "%Y/%m/%d") - \
                datetime.strptime(self.dob,    "%Y/%m/%d")).days / 365.0

class PlayerDOBMapper(IDToNameMapper):
    '''
    Not strictly a 'to-name' mapper, more of a 'to-date-of-birth' mapper,
    but let's not get stuck on details
    '''
    def __init__(self, gender, wd, idColumn, nameColumn):
        super(PlayerDOBMapper, self).__init__(wd,
                                           PLAYERS_FILE % gender,
                                           idColumn,
                                           nameColumn)
class PlayerMapper(IDToNameMapper):
    '''
    mapper for players. Note the players-file is gender-specific
    This loads more than just ID->Name mappings. See PlayerInfo
    '''
    def __init__(self, gender, wd, idColumn, nameColumn, dobColumn):
        super(PlayerMapper, self).__init__(wd,
                                           PLAYERS_FILE % gender,
                                           idColumn,
                                           nameColumn)
                                           
        self._dobMapper = PlayerDOBMapper(gender, wd, idColumn, dobColumn)
        self._id2playerinfo = dict()

    def load(self):
        '''
        multi-stage load:
         - load via our base-class call - this loads ID to NAME info
         - get our dobMapper to do the same - this loads ID to DOB info
         - create + store a PlayerInfo object populated with the fetched data
        '''
        super(PlayerMapper, self).load()
        self._dobMapper.load()
        for playerid in self._map:
            name = self._map[playerid]
            if "/" in name: continue
            if playerid not in self._dobMapper._map:
                print("WARNING: No DOB info for player %s (%s)" % (playerid,
                                                          self._map[playerid]))
                continue
            dob = self._dobMapper._map[playerid]
            pi = PlayerInfo(playerid, name, dob)
            self._id2playerinfo[playerid] = pi

    def getPlayerInfo(self, _id):
        if _id in self._id2playerinfo:
            return self._id2playerinfo[_id]
        else:
            return None




class TourMapper(IDToNameMapper):
    '''
    Less simple mapper for tours. Note the tours-file is gender-specific
    Also loads surface info from courts data and builds a TourInfo collection
    '''
    def __init__(self, gender, wd, idColumn, nameColumn, courtMapper):
        super(TourMapper, self).__init__(wd,
                                         TOURS_FILE % gender,
                                         idColumn,
                                         nameColumn)
        self._courtMapper = courtMapper
        self._toursMap = dict() # map of tourID -> TourInfo objects
    
    
    def getTourInfo(self, _id):
        return self._toursMap[_id]
        
    
    class TourInfo(object):
        def __init__(self, _id, name, surface, date, country):
            # all members public
            self._id = _id
            self.name = name
            self.surface = surface
            self.date = date
            self.country = country

    def load(self):
        # this will be a list<str> of the file minus the header
        rawDataList = None 
        # these will be the column indices we'll want to rip out
        idPosition, namePosition = -1, -1
        surfacePosition, datePosition, countryPosition = -1, -1, -1
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
                elif colName == "ID_C_T":
                    surfacePosition = curIndex
                elif colName == "DATE_T":
                    datePosition = curIndex
                elif colName == "COUNTRY_T":
                    countryPosition = curIndex
                        
                curIndex += 1
            rawDataList = f.readlines()
        
        if (idPosition == -1 or namePosition == -1 or surfacePosition == -1 or
            datePosition == -1 or countryPosition == -1):
            err = ("idPos=%s namePos=%s surfacePos=%s datePos=%s countryPos=%s"
                % (idPosition, namePosition, surfacePosition, datePosition,
                   countryPosition))
            raise BadHeaderError("Couldn't read header in %s: %s" %
                                                         (fname, err))

        if DEBUG:
            logMsg = ("idPos=%s namePos=%s surfacePos=%s datePos=%s countryPos=%s"
                % (idPosition, namePosition, surfacePosition, datePosition,
                   countryPosition))      
            print "Mapper: %s" % logMsg
        
        for rawData in rawDataList:
            elements = rawData.replace('"','').strip().split(",")
            pID, pName = elements[idPosition], elements[namePosition]
            date = elements[datePosition]
            country = elements[countryPosition]
            # we need the courtMapper to get the surface name from the ID:
            try:
                surface = self._courtMapper.getName(elements[surfacePosition])
            except:
                print "Unable to deref the court from %s in <%s>" % (
                       elements[surfacePosition], rawData)
            
            if pID and pName:
                self._map[pID] = pName # let this throw on dupes
            
            tour = TourMapper.TourInfo(pID, pName, surface, date, country)
            self._toursMap[pID] = tour
                        
        
        if DEBUG: print "Mapper: Loaded %s entries from %s" % (
                                        len(self._map), fname)

            

class RoundMapper(IDToNameMapper):
    '''
    Simple mapper for rounds (stages). Note the rounds-file is gender-neutral
    '''
    def __init__(self, wd, idColumn, nameColumn):
        super(RoundMapper, self).__init__(wd,
                                          ROUNDS_FILE,
                                          idColumn,
                                          nameColumn)

class CourtMapper(IDToNameMapper):
    '''
    Simple mapper for court types. Note the courts-file is gender neurtral
    '''
    def __init__(self, wd, idColumn, nameColumn):
        super(CourtMapper, self).__init__(wd,
                                          COURTS_FILE,
                                          idColumn,
                                          nameColumn)

class ResultInfo:
    '''
    This encapsulates info about a match's score. Create one with no args, then
    update it one set at a time by calling addSet(x, y) where x is number of
    games won by the winner of the MATCH, and y by the loser.
    You'll note that using this class is only possible if you know AHEAD OF TIME
    who the winner of the match is.
    That's fine for the purposes of our analysis in this specific program, but
    has obvious portability implications!

    The remainder of the methods in the public interface are getters of various
    kinds.
    '''
    def __init__(self):
        # Sets won by players (winner could be 3, loser 1, for example)
        # these two vars range in [0,3]
        self._winnerSetsWon = 0
        self._loserSetsWon = 0
        # Lists of games won for each player in a given set. The nth row in the
        # list corresponds to the nth set in a match.
        # these two lists will usually contain values within the range [0,6]
        self._winnerSetGames = list()
        self._loserSetGames = list()

    def addSet(self, winnerGames, loserGames):
        '''
        Updates the internal state with information about the games won
        by each player in the current (now complete) set.
        '''
        if winnerGames > loserGames:
            self._winnerSetsWon += 1
        else:
            self._loserSetsWon += 1

        self._winnerSetGames.append(int(winnerGames))
        self._loserSetGames.append(int(loserGames))
        
    def getTotalGamesPlayed(self):
        return self.getWinnerGamesWon() + self.getLoserGamesWon()
    
    def getWinnerGamesWon(self):
        result = 0
        for s in self._winnerSetGames:
            result += s
        return result
    def getLoserGamesWon(self):
        result = 0
        for s in self._loserSetGames:
            result += s
        return result
    
    def getWinnerTieBreaksWon(self):
        result = 0
        for i in xrange(len(self._winnerSetGames)):
            winnerGamesInSet = self._winnerSetGames[i]
            loserGamesInSet = self._loserSetGames[i]
            if winnerGamesInSet == 7 and loserGamesInSet == 6:
                result += 1
        return result
    
    def getLoserTieBreaksWon(self):
        result = 0
        for i in xrange(len(self._winnerSetGames)):
            winnerGamesInSet = self._winnerSetGames[i]
            loserGamesInSet = self._loserSetGames[i]
            if winnerGamesInSet == 6 and loserGamesInSet == 7:
                result += 1
        return result
        
    def __str__(self):
        return "%s,%s,%s,%s,%s,%s,%s,%s" % (
            self._winnerSetsWon,
            self._loserSetsWon,
            self.getWinnerGamesWon(),
            self.getLoserGamesWon(),
            self.getTotalGamesPlayed(),
            self.getWinnerTieBreaksWon(),
            self.getLoserTieBreaksWon(),
            self.getWinnerTieBreaksWon() + self.getLoserTieBreaksWon()
        )

class AugmentedGamesfileGenerator(object):
    '''
    This is the workhorse of the program. To use it:
     - Create one with all the required mappers passed to the c'tor
     - Call load()
     - Call dump()

    What could be easier? It defines all the internal structures used
    such as Match (represents high-level summary info of the match),
    MatchStats, which represents playing statistics for the winner and
    loser.

    '''
    def __init__(self, gender, wd, roundMapper, tourMapper, playerMapper):
        self._wd   = wd                        # working dir used to find files
        self._gamesFile = GAMES_FILE % gender  # games_<gender>.csv filename
        self._statsFile = STATS_FILE % gender  # stat_<gender>.csv filename
        self._pmap = playerMapper              # PlayerMapper instance
        self._tmap = tourMapper                # TourMapper instance
        self._rmap = roundMapper               # RoundMapper instance
        # We need to keep the order of matches played per the original input,
        # so we'll need a list to store the match keys sequentially in here:
        self._matchKeys = list()
        # Map of MatchKey -> Match
        self._matches = dict()
        # Map of MatchKey -> MatchStats
        self._stats = dict()

    class Match(object):
        '''
        Internal representation of a Match: roughly lines up with games_.csv
        (the main difference is court info being inserted in)
        Confusingly, the word Game here means match
        
        These can sometimes have just the date missing. We accept such objects
        and consider them if we can find the date of the tournament
        '''
        HEADER = "WName,Wage,LName,Lage,TourName,Surface,Country,Round,WSets,LSets," + \
                 "WGames,LGames,TotalGames,WTieBreaks,LTieBreaks," + \
                 "TotalTieBreaks,Date"

        def __init__(self, winnerI, loserI, tour, tround, surf, country, result,
                     gdate):
                     
            self._winner = winnerI
            self._loser = loserI
            self._tour = tour
            self._round = tround
            self._surface = surf
            self._country = country
            self._date = None
            try:
                self.setDate(gdate)
            except:
                pass

            # populate _result with result to start with (so if something goes
            # wrong and we try to print it, we can easily see that the issue
            # could be with the result string)
            # _result will start life as a string but change to a ResultInfo
            # if the setResult() call works - else will remain a string
            self._result = result.strip()
            self._setResultInfo(self._result)
                
        def setDate(self, dateStr):
            self._date = convertOnCourtDateToYmd(dateStr)
        
        def _setResultInfo(self, resultStr):
            '''
            resultStr is a string consisting of space-separated tokens
            representing sets. We validate the values seen and build up our
            self._result object. We leave it unchanged in the event of an
            inconsistency in the string's data
            '''
            setResults = resultStr.split(" ")
            # First make sure we have enough sets:
            if len(setResults) < 2:
                # we're uninterested if the match is incomplete
                raise ValueError("Fewer than 2 sets played in %s, skipping" %
                                                              self)

            
            ri = ResultInfo()
            for setResult in setResults:
                games = setResult.split("-")
                if len(games) != 2:
                    raise ValueError("Game result for set %s is invalid in %s"%
                                                       (setResult,       self))
                if games[0] < 6 and games[1] < 6:
                    raise ValueError("Game result for set %s is invalid in %s"%
                                                       (setResult,       self))
                # the set-score might look like 7-6(4) for tie-breaks
                # so clean that up
                if "(" in games[1]:
                    games[1] = games[1][0:games[1].index("(")]
                ri.addSet(games[0],games[1])
            
            self._result = ri

            
        @property
        def isValid(self):
            return (len(self._winner.name) and len(self._winner.dob) and
                    len (self._loser.name) and len(self._loser.dob) and
                    len(self._tour) and len(self._round) and len(self._surface) 
                    and len(self._country) and self._result is not None)
        
        @property
        def hasDate(self):
            return self._date is not None
        
        
        def __str__(self):
            return "%s,%.1f,%s,%.1f,%s,%s,%s,%s,%s,%s" % (
                    self._winner.name,
                    self._winner.getAgeAsOf(self._date),
                    self._loser.name,
                    self._loser.getAgeAsOf(self._date),
                    self._tour,
                    self._round,
                    self._surface,
                    self._country,
                    self._result,
                    self._date)
    
    class MatchStats(object):
        '''
        Internal representation of Match Stats: doesn't line up with stats_.csv
        because we'll be merging this with the Match objects. In particular,
        stats_.csv's first 4 elements uniquely identify a match, and those 4 
        elements are winnerID, loserID, tourID and roundID - which are already
        present (albeit dereferenced) in Match objects.
        
        MatchStats only consists of 2 PlayerStats instances (one for the winner
        and one for the loser), and the duration of the match (in minutes)
        class contains the statistics
        '''

        # Note that this HEADER doesn't get printed verbatim - see the
        # getFullHeader() class method in this class for how it's printed.
        HEADER = "firstSvIn,firstSvTot,aces,doubleFaults,unforcedErrs,"    \
                 "firstSvPtsWon,secondSvPtsWon,winners,breakPtsWon,"       \
                 "breakPtsTot,recvPtsWon,recvPtsTot,netApproachWon,"       \
                 "netApproachTot,totPtsWon,fastestServeKph,avgFirstSvKph," \
                 "avgSecondSvKph"
                 
        def __init__(self, winnerStats, loserStats, matchTime):
            self._winnerStats = winnerStats
            self._loserStats = loserStats
            matchTime = matchTime.replace('"','').replace("12/30/99 ","")
            if matchTime:
                timeParts = matchTime.split(":")
                minutesPlayed = int(timeParts[0])*60 + int(timeParts[1])
                self._matchTime = minutesPlayed
            else:
                self._matchTime = None
            self._suspectStats = list()
            self._validated = False # ensure we only call validate() once
        
        def validate(self):
            '''
            Updates self._suspectStats with names of columns which might have
            incorrect data. Only heuristics and consistency checks possible
            Returns: None
            Raises if called more than once
            NOTE THAT THIS IS NOT A READONLY METHOD - IT UPDATES STATE!!!
            '''
            if self._validated:
                raise Exception( "WARNING: Repeated call to MatchStats.validate()")
                
            winnerProbs = self._winnerStats.validate()
            loserProbs  = self._loserStats.validate()
            suspectStats = list()
            # if we've externally been notified of bad info through a call to
            # the public addSuspectColumn, it's already in self._suspectStats,
            # but we base our analysis on the above (local) suspectStats list.
            # So check, and if necessary, add to the local and clear the member
            if len(self._suspectStats):
                suspectStats.extend(self._suspectStats)
                self._suspectStats = list()
            
            # and now get on with the checks
            for wp in winnerProbs:
                suspectStats.append("W%s" % wp)
            for lp in loserProbs:
                suspectStats.append("L%s" % lp)
            if self._matchTime:
                if self._matchTime > 300:
                    suspectStats.append("Duration")
            else:
                suspectStats.append("Duration")
                
            if (len(suspectStats)):
                self._suspectStats.append("%s:" % len(suspectStats))
                self._suspectStats.extend(suspectStats)
            else:
                self._suspectStats.append("0")
            
            self._validated = True
                
        def addSuspectColumn(self, colName):
            '''
            We need to let users inform us of suspect columns that they know
            about but hide from us, so let them do so through this method:
            '''
            self._suspectStats.append(colName)
            
        def __str__(self):
            return "%s,%s,%s,%s" % (
                    self._winnerStats,
                    self._loserStats,
                    self._matchTime,
                    " ".join(self._suspectStats))
        
        @classmethod
        def getFullHeader(cls):
            '''
            Used by the caller program to print header info to the final
            produced file(s). It's not up to the caller what gets printed :)
            '''
            headerElems = cls.HEADER.split(",")
            if len(headerElems) != cls.PlayerStats.PLAYER_STATS_COUNT:
                raise ValueError("There are %s player stats but %s in header"
                                 % (cls.PlayerStats.PLAYER_STATS_COUNT,
                                    len(headerElems)))
            header = []
            header.extend(["W%s" % x for x in headerElems])
            header.extend(["L%s" % x for x in headerElems])
            header.append("Duration")
            header.append("SuspectColumns")
            return ",".join(header)
        
        class PlayerStats:
            '''
            A very simple class which contains match playing stats for a player
            '''
            PLAYER_STATS_COUNT = 18 # used for verification
            
            def __init__(self, firstServesIn, firstServes, aces, doubleFaults,
                         unforcedErrs, firstServePtsWon, secondServePtsWon,
                         winners, breakPtsWon, breakPts, recvPtsWon,
                         recvPts, netApproachesWon, netApproaches, totPtsWon,
                         fastestServeKph, avgFirstServeKph, avgSecondServeKph):
                
                def intorzero(i):
                    i = i.strip()
                    return int(i) if i else 0
                    
                self._firstServesIn = intorzero(firstServesIn)
                self._firstServes = intorzero(firstServes)
                self._aces = intorzero(aces)
                self._doubleFaults = intorzero(doubleFaults)
                self._unforcedErrs = intorzero(unforcedErrs)
                self._firstServePtsWon = intorzero(firstServePtsWon)
                self._secondServePtsWon = intorzero(secondServePtsWon)
                self._winners = intorzero(winners)
                self._breakPtsWon = intorzero(breakPtsWon)
                self._breakPts = intorzero(breakPts)
                self._recvPtsWon = intorzero(recvPtsWon)
                self._recvPts = intorzero(recvPts)
                self._netApproachesWon = intorzero(netApproachesWon)
                self._netApproaches = intorzero(netApproaches)
                self._totPtsWon = intorzero(totPtsWon)
                self._fastestServeKph = intorzero(fastestServeKph)
                self._avgFirstServeKph = intorzero(avgFirstServeKph)
                self._avgSecondServeKph = intorzero(avgSecondServeKph)
            
            
            def validate(self):
                '''
                Returns list of names of suspect stats. Sometimes it can only
                guess, so it's advisable to inspect related stats just in case
                '''
                issues = list()
                if (self._firstServesIn >= self._firstServes):
                    issues.append("firstSvIn")
                    issues.append("firstSvTot")
                if self._aces >= self._firstServesIn:
                    issues.append("aces")
                if self._doubleFaults > self._firstServes:
                    issues.append("doubleFaults")
                if self._firstServePtsWon > self._firstServesIn:
                    issues.append("firstSvPtsWon")
                firstServeMisses = self._firstServes - self._firstServesIn
                if self._secondServePtsWon > firstServeMisses:
                    issues.append("secondSvPtsWon")
                if self._winners > self._totPtsWon:
                    issues.append("winners")
                if (self._breakPtsWon > self._breakPts or
                    self._breakPtsWon >= self._totPtsWon):
                    issues.append("breakPtsWon")
                if (self._recvPtsWon > self._recvPts or
                    self._recvPtsWon >= self._totPtsWon):
                    issues.append("recvPtsWon")
                if (self._netApproachesWon > self._netApproaches or
                    self._netApproachesWon >= self._totPtsWon):
                    issues.append("netApproachWon")
                if (self._avgFirstServeKph >= self._fastestServeKph):
                    issues.append("avgFirstSvKph")
                    issues.append("fastestServeKph")
                if (self._avgSecondServeKph >= self._fastestServeKph or
                    self._avgSecondServeKph > self._avgFirstServeKph):
                    issues.append("avgSecondSvKph")
                    issues.append("fastestServeKph")
                return issues
            
            def __str__(self):
                return ("%s,"*self.PLAYER_STATS_COUNT)[:-1] % (
                    self._firstServesIn,
                    self._firstServes,
                    self._aces,
                    self._doubleFaults,
                    self._unforcedErrs,
                    self._firstServePtsWon,
                    self._secondServePtsWon,
                    self._winners,
                    self._breakPtsWon,
                    self._breakPts,
                    self._recvPtsWon,
                    self._recvPts,
                    self._netApproachesWon,
                    self._netApproaches,
                    self._totPtsWon,
                    self._fastestServeKph,
                    self._avgFirstServeKph,
                    self._avgSecondServeKph,
                )

    def load(self):
        '''
        This reads the games and stats files, dereferencing all the pesky IDs to
        names on the fly and joins up the MatchStats correspondingly.
        '''
        # this will be a list<Match> of the file minus the header
        matchList = None 
        
        fname = "%s/%s" % (self._wd, self._gamesFile)
        if DEBUG: print "AugmentedGamesfileGenerator: Reading %s" % fname
        with open(fname , "r") as f:
            f.readline() # skips the header
            matchList = f.readlines()
        
        suspectDates = dict() # map of matchKey -> bool
        for matchStr in matchList: # matchStr is a csv-string
            elements = matchStr.replace('"','').strip().split(",")
            (winnerID, loserID, tourID, roundID) = (elements[0],
                                                    elements[1],
                                                    elements[2],
                                                    elements[3])
            matchKey = MatchKey(winnerID, loserID, tourID, roundID)
            tourInfo = self._tmap.getTourInfo(tourID)
            winnerInfo = self._pmap.getPlayerInfo(winnerID)
            loserInfo  = self._pmap.getPlayerInfo(loserID)
            if not winnerInfo or not loserInfo:
                print "WARNING: Incomplete player info for player(s) in %s" % (
                                                                      matchKey)
                continue
            # is this a doubles match?
            if '/' in winnerInfo.name and '/' in loserInfo.name:
                # yes it is, drop
                continue
            matchDateStr = elements[5]
            # sometime the matches don't have dates, so use tour-start date
            # and be sure to log this
            if not matchDateStr:
                matchDateStr = tourInfo.date
                print ("INFO: Match date for {0} missing, using tour-date %s".
                        format(matchKey) % matchDateStr)
                if not matchDateStr:
                    raise ValueError("No match or tour date for %s" % matchKey)

            try:
                match = AugmentedGamesfileGenerator.Match(
                        winnerInfo,
                        loserInfo,
                        tourInfo.name,
                        tourInfo.surface,
                        tourInfo.country,
                        self._rmap.getName(roundID),
                        elements[4], # result-string
                        matchDateStr)
                
                self._matches[matchKey] = match
                self._matchKeys.append(matchKey)
            except Exception as e:
                print "Skipping match %s: %s" % (matchKey, e)

        # end for-loop

        if DEBUG:
            print "AugmentedGamesfileGenerator: Loaded %s/%s (%.1f) matches" % (
                         len(self._matches),
                         len(matchList),
                         len(self._matches)*100.0/len(matchList))
        
        # now load in the stats
        statsList = None # this will be a list<MatchStats> of the file
        fname = "%s/%s" % (self._wd, self._statsFile)
        if DEBUG: print "AugmentedGamesfileGenerator: Reading %s" % fname
        with open(fname, "r") as f:
            f.readline() # skips the header
            statsList = f.readlines()
        
        for statsStr in statsList: # statsStr is a csv-string
            elements = statsStr.replace('"','').strip().split(",")
            (winnerID, loserID, tourID, roundID) = (elements[0],
                                                    elements[1],
                                                    elements[2],
                                                    elements[3])
            matchKey = MatchKey(winnerID, loserID, tourID, roundID)
            winnerStats = AugmentedGamesfileGenerator.MatchStats.PlayerStats(
                            elements[4], elements[5],  # 1st server in, total
                            elements[6], elements[7],  # aces, doublefaults
                            elements[8],               # unforced errors
                            elements[9], elements[11], # 1st serv winners, 2nd
                            elements[13],              # winners
                            elements[14], elements[15], # breakPts won, tot
                            elements[40], elements[41], # recvPtsWon, tot
                            elements[16], elements[17], # netApproach won, tot
                            elements[18],               # total pts won
                            elements[19],               # fastest serve kph
                            elements[20], elements[21]) # avg 1st sv kph, 2nd
                            
            loserStats = AugmentedGamesfileGenerator.MatchStats.PlayerStats(
                            elements[22], elements[23], # 1st server in, total
                            elements[24], elements[25], # aces, doublefaults
                            elements[26],               # unforced errors
                            elements[27], elements[29], # 1st serv winners, 2nd
                            elements[31],               # winners
                            elements[32], elements[33], # breakPts won, tot
                            elements[42], elements[43], # recvPtsWon, tot
                            elements[34], elements[35], # netApproach won, tot
                            elements[36],               # total pts won
                            elements[37],               # fastest serve kph
                            elements[38], elements[39]) # avg 1st sv kph, 2nd
            
            matchStats = AugmentedGamesfileGenerator.MatchStats(winnerStats,
                                                                loserStats,
                                                                elements[44])
            if matchKey in suspectDates:
                matchStats.addSuspectColumn("Date")
            matchStats.validate()
            self._stats[matchKey] = matchStats
        
        if DEBUG: print "AugmentedGamesfileGenerator: Loaded %s stats" % (
                                                             len(self._stats))

    def _createDummyStats(cls):
        '''
        Returns a long string filled with n/a
        '''
        cols=AugmentedGamesfileGenerator.MatchStats.getFullHeader().split(",")
        return ",".join(["n/a" for x in cols])
    
    def dump(self, destPath):
        '''
        Writes the in-memory version of the matchdata to disk
        '''
        if DEBUG: print "AugmentedGamesfileGenerator: Dumping to %s" % destPath
        outfile = open(destPath, "wb")
        
        # write out the headers
        # we stick everything into a list so that we can see how big
        # a header is - then we ensure that no rows get written with
        # a size different to that, else we've got a bug
        # slightly belaboured but makes debugging a lot easier if needed
        outfileHeader = "%s,%s,%s" % (
                            AugmentedGamesfileGenerator.Match.HEADER,
                            AugmentedGamesfileGenerator.MatchStats.getFullHeader(),
                            "MatchKey")
        outfile.write("%s\n" % outfileHeader)
        nHeaderColumns = len(outfileHeader.strip().split(","))
        #
        # and now write out the meat - buffer things to let us go quickly
        bufSz = 5000
        buf = list()
        matchesWritten = 0
        dummyStatsObject = self._createDummyStats()
        for matchKey in self._matchKeys:
            match = self._matches[matchKey]            
            if matchKey in self._stats:
                stats = self._stats[matchKey]
            else:
                stats = dummyStatsObject
                
            fullMatchRow = "%s,%s,%s" % (match, stats, matchKey)
            if len(fullMatchRow.split(",")) != nHeaderColumns:
                # the shit has hit the fan!
                raise Exception("Header has %s columns but a row has %s: Row=%s" % (
                    nHeaderColumns, len(fullMatchRow.split(",")), fullMatchRow))
            buf.append(fullMatchRow)
            if (len(buf) == bufSz):
                outfile.write("\n".join(buf))
                outfile.write("\n")
                matchesWritten += len(buf)
                buf = list()
        # at the end of the loop we'll most likely still have some left in buf
        outfile.write("\n".join(buf))
        matchesWritten += len(buf)
        outfile.write("\n")
        outfile.close()
        if DEBUG: print "AugmentedGamesfileGenerator: Dumped, wrote %s" % (
                                                              matchesWritten)
    
    
def doMain():
    genderCodes = ["atp", # men's
                   "wta"] # women's
    # genderCodes = ['atp']
    rawCsvDir = "/home/mzc/dev/tennis/oncourt/data/rawcsv" # source dir
    outCsvDir = "/home/mzc/dev/tennis/oncourt/data/csv"    # dest dir
    
    roundMapper = RoundMapper(rawCsvDir, "ID_R", "NAME_R")
    roundMapper.load()
    
    courtMapper = CourtMapper(rawCsvDir, "ID_C", "NAME_C")
    courtMapper.load()

    for gender in genderCodes:
        playerMapper = PlayerMapper(gender, rawCsvDir, "ID_P", "NAME_P", "DATE_P")
        playerMapper.load()
    
        tourMapper = TourMapper(gender, rawCsvDir, "ID_T", "NAME_T",
                                courtMapper)
        tourMapper.load()
    
    
        agg = AugmentedGamesfileGenerator(gender, rawCsvDir,
                                          roundMapper, tourMapper, playerMapper)
        agg.load()
        agg.dump("%s/augmented_games_%s.csv" % (outCsvDir, gender))
                              
    return 0

if __name__ == "__main__":
   doMain()
